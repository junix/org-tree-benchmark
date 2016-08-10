module SQL where
import Database.HDBC
import Org
import Data.List (intercalate)

depTab  orgId = "DepartmentOrg" ++ show orgId
memTab  orgId = "MemberOrg"     ++ show orgId
treeTab orgId = "TreeOrg"       ++ show orgId
pathTab orgId = "PathOrg"       ++ show orgId

ddl orgId =
    [ "CREATE TABLE " ++ depTab  orgId ++ " (ID CHAR(32) PRIMARY KEY, TYPE INT NOT NULL)"
    , "CREATE TABLE " ++ memTab  orgId ++ " (ID CHAR(32) PRIMARY KEY, AGE INT)"
    , "CREATE TABLE " ++ treeTab orgId ++ " (NODE_ID CHAR(32), NODE_TYPE INT, PARENT_ID CHAR(32), PARENT_TYPE INT" ++
      fref "PARENT_ID" ++ ")"
    , "CREATE TABLE " ++ pathTab orgId ++ " (NODE_ID CHAR(32), NODE_TYPE INT, PARENT_ID CHAR(32), PARENT_TYPE INT" ++
      fref "NODE_ID" ++ fref "PARENT_ID" ++ ")"
    ]
    where fref x = ",FOREIGN KEY ("++x++") REFERENCES " ++ depTab orgId ++ " (ID)"

st2i :: Entity -> SqlValue
st2i = toSql . t2i

seid :: Entity -> SqlValue
seid = toSql.eid

value :: Entity -> [SqlValue]
value e@(Member _ id') = [seid e, toSql id']
value e = [seid e, st2i e]

istmt (Member     org _) = "INSERT INTO " ++ memTab org ++ " VALUES (?, ?)"
istmt (Department org _) = "INSERT INTO " ++ depTab org ++ " VALUES (?, ?)"
istmt (SubCompany org _) = "INSERT INTO " ++ depTab org ++ " VALUES (?, ?)"

joinPathSQL (Member _ _) dep = ""
joinPathSQL who dep
    | oid who /= oid dep = ""
    | otherwise = concat
        [ "INSERT INTO ", ptab, " "
        , "SELECT ", mkFields [quote cid, ctype, quote pid, ptype], " UNION "
        , "SELECT ", mkFields [quote cid, ctype, "PARENT_ID", "PARENT_TYPE"], " FROM ", ptab, " "
        , "WHERE ", eqNodeExp pid ptype
        ]
        where ptab = pathTab.oid $ dep
              cid   = eid who
              ctype = (show.t2i) who
              pid   = eid dep
              ptype = (show.t2i) dep

mkFields :: [String] -> String
mkFields = intercalate ","

eqNodeExp   nid ntype =  " NODE_ID = "   ++ quote nid ++ " AND NODE_TYPE = "   ++ ntype
eqParentExp nid ntype =  " PARENT_ID = " ++ quote nid ++ " AND PARENT_TYPE = " ++ ntype

quote :: String -> String
quote s = '\'' : s ++ "'"

movPath (Member _ _) dep = ""
movPath who dep = concat
    [ "INSERT INTO PATH ( "
    , "VALUES (", mkFields [quote cid, ctype, quote pid, ptype], ") UNION "
    , "(SELECT ", mkFields [quote cid, ctype, "PARENT_ID", "PARENT_TYPE"], " FROM PATH WHERE ", eqNodeExp   pid ptype, ") UNION "
    , "(SELECT ", mkFields ["NODE_ID", "NODE_TYPE", quote pid, ptype],     " FROM PATH WHERE ", eqParentExp cid ctype, ") UNION "
    , "(SELECT ", mkFields ["A.NODE_ID","A.NODE_TYPE", "B.PARENT_ID", "B.PARENT_TYPE"],  " FROM PATH A, PATH B WHERE "
    , "A.PARENT_ID = ", quote cid, " AND B.NODE_ID = " , quote pid
    , ")"
    ]
    where cid   = eid who
          ctype = (show.t2i) who
          pid   = eid dep
          ptype = (show.t2i) dep

querySubCntStmt (Department orgId _) = "SELECT COUNT(*) FROM " ++ treeTab orgId ++ " WHERE NODE_ID IN " ++
                                       "(SELECT NODE_ID FROM " ++ pathTab orgId ++
                                       " WHERE PARENT_ID = ? AND NODE_ID != PARENT_ID)"

queryParentsStmt who = "SELECT NODE_ID FROM " ++ treeTab org ++ " WHERE NODE_ID IN " ++
                        "(SELECT PARENT_ID FROM " ++ pathTab org ++" WHERE NODE_ID = ?)"
    where org = oid who
