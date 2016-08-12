{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ExtendedDefaultRules #-}

module SQL where
import Database.HDBC
import Org
import Data.List (intercalate)
import Text.InterpolatedString.Perl6 (qc,qq)

shardSize = 97

shard orgId = orgId `rem` shardSize

depTab  orgId = "department" ++ (show.shard) orgId
memTab  orgId = "member"     ++ (show.shard) orgId
treeTab orgId = "tree"       ++ (show.shard) orgId
pathTab orgId = "path"       ++ (show.shard) orgId

pathTabOf :: Entity -> String
pathTabOf = pathTab.oid


data FieldType  = C Int | I
data Constraint = NOT_NULL | KEY
data Field      = CField String FieldType [Constraint]
data FK         = FK { field   :: String
                     , refTab  :: String
                     , refName :: String
                     } deriving (Show)
data Tab        = Tab { name          :: String
                      , fields        :: [Field]
                      , foreign_keys  :: [FK]
                      } deriving (Show)

type OrgId = Integer
type ShardId = Integer

instance Show FieldType where
    show (C n) = "CHAR(" ++ show n ++ ")"
    show I = "INT"

instance Show Constraint where
    show NOT_NULL = "NOT NULL"
    show KEY = "PRIMARY KEY"

instance Show Field where
   show (CField name fieldType cs) = name ++ " " ++ show fieldType ++ " " ++  (intercalate " " . map show $ cs)

-- tables
department :: ShardId -> Tab
department shard =
    Tab { name   = "department" ++ show shard
        , fields = [ CField "ID"     (C 32)  [KEY     ]
                   , CField "ORG_ID" (C 32)  [NOT_NULL]
                   , CField "TYPE"   I       [NOT_NULL]
                   ]
        , foreign_keys = []
        }

member :: ShardId -> Tab
member shard =
    Tab { name   = "member" ++ show shard
        , fields = [ CField "ID"      (C 32) [KEY     ]
                   , CField "ORG_ID"  (C 32) [NOT_NULL]
                   , CField "AGE"     I      [NOT_NULL]
                   ]
        , foreign_keys = []
        }

tree :: ShardId -> Tab
tree shard =
    Tab { name   = "tree" ++ show shard
        , fields = [ CField "ORG_ID"      (C 32) [NOT_NULL]
                   , CField "NODE_ID"     (C 32) [KEY     ]
                   , CField "NODE_TYPE"   I      [NOT_NULL]
                   , CField "PARENT_ID"   (C 32) [NOT_NULL]
                   , CField "PARENT_TYPE" I      [NOT_NULL]
                   ]
        , foreign_keys = [FK { field   = "PARENT_ID", refTab  = "department"++show shard, refName = "ID"}]
        }

path :: ShardId -> Tab
path shard =
    Tab { name   = "path" ++ show shard
        , fields = [ CField "ORG_ID"      (C 32) [NOT_NULL]
                   , CField "NODE_ID"     (C 32) [NOT_NULL]
                   , CField "NODE_TYPE"   I      [NOT_NULL]
                   , CField "PARENT_ID"   (C 32) [NOT_NULL]
                   , CField "PARENT_TYPE" I      [NOT_NULL]
                   ]
        , foreign_keys = [ FK { field   = "PARENT_ID", refTab  = "department"++show shard, refName = "ID" }
                         , FK { field   = "NODE_ID",   refTab  = "department"++show shard, refName = "ID" }
                         ]
         }

ddlOf :: Tab -> String
ddlOf (Tab { name = tabName, fields = fs, foreign_keys = fks}) =
        [qc| CREATE TABLE {tabName} (
                {intercalate "," . map show $ fs}
                {concatMap fref fks}
             )
        |]
    where fref :: FK -> String
          fref (FK { field = f, refTab = t, refName = n}) = [qc|, FOREIGN KEY ({f}) REFERENCES {t}  ({n}) |]

ddl orgId = map ddlOf. map ($orgId) $ [department, member, tree, path]

st2i :: Entity -> SqlValue
st2i = toSql . t2i

seid :: Entity -> SqlValue
seid = toSql.eid

value :: Entity -> [SqlValue]
value e@(Member _ id') = [seid e, (toSql.soid) e, toSql id']
value e = [seid e, (toSql.soid) e, st2i e]

insertStmt :: Entity -> String
insertStmt (Member     org _) = [qc| REPLACE INTO {memTab org} VALUES (?, ?, ?) |]
insertStmt (Department org _) = [qc| REPLACE INTO {depTab org} VALUES (?, ?, ?) |]
insertStmt (SubCompany org _) = [qc| REPLACE INTO {depTab org} VALUES (?, ?, ?) |]

joinPathSQL :: Entity -> Entity -> String
joinPathSQL (Member _ _) dep = ""
joinPathSQL who dep
    | oid who /= oid dep = ""
    | otherwise =
        [qc| INSERT INTO {ptab}
             SELECT '{orgid}', '{cid}', {ctype}, '{pid}', {ptype}
             UNION
             SELECT ORG_ID, '{cid}', {ctype}, PARENT_ID, PARENT_TYPE FROM {ptab}
                WHERE ORG_ID  = '{orgid}' AND
                      NODE_ID = '{pid}'   AND
                      NODE_TYPE = {ptype}
        |]
        where ptab  = pathTab.oid $ dep
              orgid = soid dep
              cid   = eid who
              ctype = (show.t2i) who
              pid   = eid dep
              ptype = (show.t2i) dep

mkFields :: [String] -> String
mkFields = intercalate ","

eqNodeExp :: String -> String -> String -> String
eqNodeExp orgid nid ntype =  [qc| ORG_ID = {quote orgid} AND NODE_ID = {quote nid} AND NODE_TYPE = {ntype} |]

eqParentExp :: String -> String -> String -> String
eqParentExp orgid nid ntype =  [qc| ORG_ID = {quote orgid} AND PARENT_ID = {quote nid} AND PARENT_TYPE = {ntype} |]

quote :: String -> String
quote s = '\'' : s ++ "'"

{-
movPathSQL1 (Member _ _) dep = ""
movPathSQL1 who dep = concat
    [ "DELETE FROM ", tab, " WHERE "
    , "(NODE_ID = ", quote cid, " OR NODE_ID IN "
    , "(SELECT NODE_ID FROM ", tab, " WHERE PARENT_ID = ", quote cid, ")) AND "
    , "PARENT_ID IN (SELECT PARENT FROM pATH WHERE iD = X)
]
    where cid   = eid who
          tab = pathTabOf who
          orgid = soid dep
          qoid = quote orgid
          ctype = (show.t2i) who
          pid   = eid dep
          ptype = (show.t2i) dep
-}

movPathSQL (Member _ _) dep = ""
movPathSQL who dep = concat
    [ "INSERT INTO ", tab
    , "SELECT ", mkFields [qoid, quote cid, ctype, quote pid, ptype], " UNION "
    , "SELECT ", mkFields [qoid, quote cid, ctype, "PARENT_ID", "PARENT_TYPE"], " FROM "
    , tab, " WHERE ", eqNodeExp orgid pid ptype, " UNION "
    , "SELECT ", mkFields [qoid, "NODE_ID", "NODE_TYPE", quote pid, ptype]
    , " FROM ", tab, " WHERE ", eqParentExp orgid cid ctype, " UNION "
    , "SELECT ", mkFields [qoid, "A.NODE_ID","A.NODE_TYPE", "B.PARENT_ID", "B.PARENT_TYPE"]
    , " FROM ",tab," A, ",tab, " B WHERE "
    , "A.PARENT_ID = ", quote cid, " AND B.NODE_ID = " , quote pid, ""
    ]
    where cid   = eid who
          tab = pathTabOf who
          orgid = soid dep
          qoid = quote orgid
          ctype = (show.t2i) who
          pid   = eid dep
          ptype = (show.t2i) dep

querySubCntStmt e@(Department orgId _) =
    [qq| SELECT COUNT(*) FROM $treeTable WHERE
            ORG_ID = $strOid AND
            NODE_ID IN
              (SELECT NODE_ID FROM $pathTable
                WHERE PARENT_ID = ? AND NODE_ID != PARENT_ID)
    |]
    where treeTable = treeTab orgId
          pathTable = pathTab orgId
          strOid     = (quote.soid) e

queryParentsStmt who =
    [qc| SELECT NODE_ID FROM {treeTable}
         WHERE ORG_ID = '{orgid}' AND
               NODE_ID IN (SELECT PARENT_ID FROM  {pathTable} WHERE NODE_ID = ?)
    |]
    where org   = oid who
          treeTable = treeTab org
          pathTable = pathTab org
          orgid = soid who

