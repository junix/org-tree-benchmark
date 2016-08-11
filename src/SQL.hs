{-# LANGUAGE OverloadedStrings #-}
module SQL where
import Database.HDBC
import Org
import Data.List (intercalate)

shardSize = 100

shard orgId = orgId `rem` shardSize

depTab  orgId = "department" ++ (show.shard) orgId
memTab  orgId = "member"     ++ (show.shard) orgId
treeTab orgId = "tree"       ++ (show.shard) orgId
pathTab orgId = "path"       ++ (show.shard) orgId

data FieldType  = C Int | I
data Constraint = NOT_NULL | KEY
data Field      = CField String FieldType [Constraint] | Field String FieldType
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
   show (Field name fieldType) = name ++ " " ++ show fieldType

department :: ShardId -> Tab
department shard = Tab { name   = "department" ++ show shard
                       , fields = [ CField "ID"     (C 32)  [KEY]
                                  , CField "ORG_ID" (C 32)  [NOT_NULL]
                                  , CField "TYPE"   I       [NOT_NULL]
                                  ]
                       , foreign_keys = []
                       }

member :: ShardId -> Tab
member shard = Tab { name   = "member" ++ show shard
                   , fields = [ CField "ID"      (C 32) [KEY]
                              , CField "ORG_ID"  (C 32) [NOT_NULL]
                              , CField "AGE"     I      [NOT_NULL]
                              ]
                   , foreign_keys = []
                   }

tree :: ShardId -> Tab
tree shard = Tab { name   = "tree" ++ show shard
                 , fields = [ CField "ORG_ID"      (C 32) [NOT_NULL]
                            , CField "NODE_ID"     (C 32) [NOT_NULL]
                            , CField "NODE_TYPE"   I      [NOT_NULL]
                            , CField "PARENT_ID"   (C 32) [NOT_NULL]
                            , CField "PARENT_TYPE" I      [NOT_NULL]
                            ]
                   , foreign_keys = [FK { field   = "PARENT_ID", refTab  = "department"++show shard, refName = "ID"}]
                   }

path :: ShardId -> Tab
path shard = Tab { name   = "path" ++ show shard
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
        "CREATE TABLE " ++ tabName ++ " (" ++
        (intercalate "," . map show $ fs) ++
        concatMap fref fks ++ ")"
    where fref (FK { field = f, refTab = t, refName = n}) =
            ",FOREIGN KEY (" ++ f ++ ") REFERENCES " ++ t ++ " (" ++ n ++ ")"

ddl orgId = map ddlOf. map ($orgId) $ [department, member, tree, path]

st2i :: Entity -> SqlValue
st2i = toSql . t2i

seid :: Entity -> SqlValue
seid = toSql.eid

value :: Entity -> [SqlValue]
value e@(Member _ id') = [seid e, (toSql.soid) e, toSql id']
value e = [seid e, (toSql.soid) e, st2i e]

istmt (Member     org _) = "INSERT INTO " ++ memTab org ++ " VALUES (?, ?, ?)"
istmt (Department org _) = "INSERT INTO " ++ depTab org ++ " VALUES (?, ?, ?)"
istmt (SubCompany org _) = "INSERT INTO " ++ depTab org ++ " VALUES (?, ?, ?)"

joinPathSQL (Member _ _) dep = ""
joinPathSQL who dep
    | oid who /= oid dep = ""
    | otherwise = concat
        [ "INSERT INTO ", ptab, " "
        , "SELECT ", mkFields [quote orgid, quote cid, ctype, quote pid, ptype], " UNION "
        , "SELECT ", mkFields ["ORG_ID", quote cid, ctype, "PARENT_ID", "PARENT_TYPE"], " FROM ", ptab, " "
        , "WHERE ", eqNodeExp orgid pid ptype
        ]
        where ptab = pathTab.oid $ dep
              orgid = soid dep
              cid   = eid who
              ctype = (show.t2i) who
              pid   = eid dep
              ptype = (show.t2i) dep

mkFields :: [String] -> String
mkFields = intercalate ","

eqNodeExp   orgid nid ntype =  " ORG_ID = " ++ quote orgid ++ " AND NODE_ID = "   ++ quote nid ++ " AND NODE_TYPE = " ++ ntype
eqParentExp orgid nid ntype =  " ORG_ID = " ++ quote orgid ++ " AND PARENT_ID = " ++ quote nid ++ " AND PARENT_TYPE = " ++ ntype

quote :: String -> String
quote s = '\'' : s ++ "'"

movPath (Member _ _) dep = ""
movPath who dep = concat
    [ "INSERT INTO PATH ( "
    , "VALUES (", mkFields [quote cid, ctype, quote pid, ptype], ") UNION "
    , "(SELECT ", mkFields [quote cid, ctype, "PARENT_ID", "PARENT_TYPE"], " FROM PATH WHERE ", eqNodeExp   orgid pid ptype, ") UNION "
    , "(SELECT ", mkFields ["NODE_ID", "NODE_TYPE", quote pid, ptype],     " FROM PATH WHERE ", eqParentExp orgid cid ctype, ") UNION "
    , "(SELECT ", mkFields ["A.NODE_ID","A.NODE_TYPE", "B.PARENT_ID", "B.PARENT_TYPE"],  " FROM PATH A, PATH B WHERE "
    , "A.PARENT_ID = ", quote cid, " AND B.NODE_ID = " , quote pid
    , ")"
    ]
    where cid   = eid who
          orgid = soid dep
          ctype = (show.t2i) who
          pid   = eid dep
          ptype = (show.t2i) dep

querySubCntStmt e@(Department orgId _) =
    "SELECT COUNT(*) FROM " ++ treeTab orgId ++ " WHERE ORG_ID = " ++ (quote.soid) e ++
    " AND NODE_ID IN " ++
    "(SELECT NODE_ID FROM " ++ pathTab orgId ++ " WHERE PARENT_ID = ? AND NODE_ID != PARENT_ID)"

queryParentsStmt who =
    "SELECT NODE_ID FROM " ++ treeTab org ++ " WHERE ORG_ID = " ++ quote orgid ++
    " AND NODE_ID IN " ++ "(SELECT PARENT_ID FROM " ++ pathTab org ++" WHERE NODE_ID = ?)"
    where org   = oid who
          orgid = soid who

