module Main where
import Database.HDBC
import Database.HDBC.MySQL
import Data.Maybe
import NiceFork(parRun)
import SplitR
import Org
import SQL

createOrgTabs' orgId conn = do
    mapM_ (\s -> run conn s []) (ddl orgId)
    commit conn

createOrgTabs orgId = withConn (createOrgTabs' orgId)

dropOrgTabs' orgId conn = do
    let ddls = map (("drop table "++).($orgId)) [pathTab, treeTab, depTab, memTab]
    mapM_ (\s -> run conn s []) ddls
    commit conn

dropOrgTabs orgId = withConn (dropOrgTabs' orgId)

main = return ()

--conn = connectPostgreSQL "host=192.168.99.100 dbname=hello user=postgres"
conn = connectMySQL defaultMySQLConnectInfo { mysqlHost     = "127.0.0.1"
                                            , mysqlUser     = "root"
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

joinIn who dep = withConn (join' who dep)

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

createOrg' orgId level subCnt conn = do
   let root = Department orgId 1
   new' [root] conn
   join' root root conn
   itree' level subCnt root conn

createOrg orgId level ccnt  = withConn (createOrg' orgId level ccnt) >> return ()

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

createOrgs orgs level subCnt = do
    cn <- conn
    mapM_ (\org -> createOrg' org level subCnt cn) orgs
    disconnect cn
    return ()

-- parallel operations
parCreateTabs oss = parRun (map createTabs oss)

parDropTabs oss = parRun (map dropTabs oss)

parCreateOrgs oss level subCnt = parRun $
    map (\os ->createOrgs os level subCnt) oss
