module Main where
import Database.HDBC
--import Database.HDBC.PostgreSQL
import Database.HDBC.MySQL
import Data.List (intercalate)
import Data.Maybe

data Entity = Department Int Int
            | SubCompany Int Int
            | Member Int Int
            deriving(Show,Eq,Read,Ord)

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

createOrgTabs' orgId conn = do
    mapM_ (\s -> run conn s []) (ddl orgId)
    commit conn

createOrgTabs orgId = withConn (createOrgTabs' orgId)

dropOrgTabs' orgId conn = do
    let ddls = map (("drop table "++).($orgId)) [pathTab, treeTab, depTab, memTab]
    mapM_ (\s -> run conn s []) ddls
    commit conn

dropOrgTabs orgId = withConn (dropOrgTabs' orgId)


t2i :: Entity -> Int
t2i (Member _ _)     = 0
t2i (Department _ _) = 1
t2i (SubCompany _ _) = 2

st2i :: Entity -> SqlValue
st2i = toSql . t2i

eid :: Entity -> String
eid (Member _ id')     = 'm':show id'
eid (Department _ id') = 'd':show id'
eid (SubCompany _ id') = 's':show id'

oid :: Entity -> Int
oid (Member     org _) = org
oid (Department org _) = org
oid (SubCompany org _) = org

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

main = return ()

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

--conn = connectPostgreSQL "host=192.168.99.100 dbname=hello user=postgres"
conn = connectMySQL defaultMySQLConnectInfo { mysqlHost     = "127.0.0.1"
                                            , mysqlUser     = "jun"
                                            , mysqlPassword = "" }


exe :: String -> [SqlValue] -> Connection -> IO Integer
exe stmt args conn = do
    r <- run conn stmt args
    disconnect conn
    return r

new' :: [Entity] -> Connection -> IO Integer
new' [] conn = return 0
new' es@(e:_) conn = do
    stmt <- prepare conn (istmt e)
    rs <- mapM (execute stmt . value) es
    commit conn
    return (head rs)

new :: [Entity] -> IO Integer
new es = withConn (new' es)

withConn act = do
   cn <- conn
   rs <- act cn
   disconnect cn
   return rs

{--------------------------------------------------------------------------------------------
               QUERIES
--------------------------------------------------------------------------------------------}
querySubCnt' who conn = do
    rs <- quickQuery' conn (querySubCntStmt who) [seid who]
    let cnt = listToMaybe . map fromSql. concat $ rs :: Maybe Integer
    return cnt

querySubCnt who = withConn (querySubCnt' who)

queryParent' who conn = do
    rs <- quickQuery' conn (queryParentsStmt who) [seid who]
    let ps = map sqlid2Id . concat $ rs
    return ps

queryParent who = withConn (queryParent' who)

join' who dep conn = do
    let rec = [seid who, st2i who, seid dep, st2i dep]
        sql = joinPathSQL who dep
        tab = treeTab.oid $ dep
        isql= "INSERT INTO " ++ tab ++ " VALUES (?,?,?,?)"
    run conn isql rec
    if null sql
        then commit conn
        else do
                run conn sql []
                commit conn

join who dep = withConn (join' who dep)

genChildren (Department org n) cnt = map (Department org . (n*10+)) [0..cnt-1]
genEmplee   (Department org n) cnt = map (Member org. (n*10+)) [0..cnt-1]

itree' level ccnt parent@(Department org pid) conn
    | pid > 10^level = return ()
    | otherwise = do
        let cs = genChildren parent ccnt
        let es = genEmplee parent ccnt
        new' cs conn
        new' es conn
        mapM_ (\c -> join' c parent conn) es
        mapM_ (\c -> join' c parent conn) cs
        mapM_ (\c -> itree' level ccnt c conn) cs
        return ()

createTree' orgId level subCnt conn = do
   let root = Department orgId 1
   new' [root] conn
   join' root root conn
   itree' level subCnt root conn

createOrg orgId level ccnt  = withConn (createTree' orgId level ccnt) >> return ()

sqlid2Id :: SqlValue -> Integer
sqlid2Id sid = read . filter (`elem` ['0'..'9']) $ s :: Integer
    where s =  fromSql sid :: String

clear' orgId conn = do
    let exps = map (("DELETE FROM "++) . ($orgId)) [pathTab, treeTab, depTab, memTab]
    mapM_ (\stmt -> run conn stmt []) exps
    commit conn

clear orgId = withConn (clear' orgId)

createTabs' orgs cn = mapM_ (\org -> createOrgTabs' org cn) orgs
dropTabs'   orgs cn = mapM_ (\org -> dropOrgTabs'   org cn) orgs

createTabs orgs = withConn (createTabs' orgs)
dropTabs   orgs = withConn (dropTabs'   orgs)

createOrgs :: [Int] -> IO ()
createOrgs = mapM_ (\org -> createOrg org 1 1)