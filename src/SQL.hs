{-# LANGUAGE OverloadedStrings #-}
module SQL where
import Database.HDBC
import Org
import Data.List (intercalate)

shardSize = 10

shard orgId = orgId `rem` shardSize

depTab  orgId = "DepartmentOrg" ++ (show.shard) orgId
memTab  orgId = "MemberOrg"     ++ (show.shard) orgId
treeTab orgId = "TreeOrg"       ++ (show.shard) orgId
pathTab orgId = "PathOrg"       ++ (show.shard) orgId

ddl orgId =
    [ "CREATE TABLE " ++ depTab  orgId ++ " (ID CHAR(32) PRIMARY KEY, ORG_ID CHAR(32), TYPE INT NOT NULL)"
    , "CREATE TABLE " ++ memTab  orgId ++ " (ID CHAR(32) PRIMARY KEY, ORG_ID CHAR(32), AGE INT)"
    , "CREATE TABLE " ++ treeTab orgId ++ " (ORG_ID CHAR(32), NODE_ID CHAR(32), NODE_TYPE INT, PARENT_ID CHAR(32), PARENT_TYPE INT" ++
      fref "PARENT_ID" ++ ")"
    , "CREATE TABLE " ++ pathTab orgId ++ " (ORG_ID CHAR(32), NODE_ID CHAR(32), NODE_TYPE INT, PARENT_ID CHAR(32), PARENT_TYPE INT" ++
      fref "NODE_ID" ++ fref "PARENT_ID" ++ ")"
    ]
    where fref x = ",FOREIGN KEY ("++x++") REFERENCES " ++ depTab orgId ++ " (ID)"

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
    " AND NODE_ID IN " ++ "(SELECT NODE_ID FROM " ++ pathTab orgId ++
    " WHERE PARENT_ID = ? AND NODE_ID != PARENT_ID)"


queryParentsStmt who = "SELECT NODE_ID FROM " ++ treeTab org ++ " WHERE ORG_ID = " ++ quote orgid ++
                       " AND NODE_ID IN " ++
                        "(SELECT PARENT_ID FROM " ++ pathTab org ++" WHERE NODE_ID = ?)"
    where org = oid who
          orgid = soid who

