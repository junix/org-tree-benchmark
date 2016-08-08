module Main where
import Database.HDBC
import Database.HDBC.PostgreSQL
import Data.Maybe

data Entity = Department Int | SubCompany Int | Member Int deriving(Show,Eq,Read,Ord)

t2i :: Entity -> Int
t2i (Member _)     = 0
t2i (Department _) = 1
t2i (SubCompany _) = 2

st2i :: Entity -> SqlValue
st2i = toSql . t2i

eid :: Entity -> String
eid (Member id')     = 'm':show id'
eid (Department id') = 'd':show id'
eid (SubCompany id') = 's':show id'

seid :: Entity -> SqlValue
seid = toSql.eid

value :: Entity -> [SqlValue]
value e@(Member id') = [seid e, toSql id']
value e = [seid e, st2i e]

istmt (Member _)     = "INSERT INTO member VALUES (?, ?)"
istmt (Department _) = "INSERT INTO department VALUES (?, ?)"
istmt (SubCompany _) = "INSERT INTO department VALUES (?, ?)"

joinPath (Member _) dep = ""
joinPath who dep = concat
    [ "INSERT INTO PATH (VALUES ('"++ nid, "',", sntype, ",'", pid, "',", sptype
    , ") UNION Select '", nid, "',", sntype
    , ", PARENT_ID, PARENT_TYPE FROM PATH WHERE NODE_ID = '", pid
    , "' AND NODE_TYPE = ", sptype, ")"
    ]
    where nid    = eid who
          sntype = (show.t2i) who
          pid    = eid dep
          sptype = (show.t2i) dep

querySubCntStmt (Department _) = "select count(*) from tree where node_id in (select node_id from path where parent_id = ?)"
queryParentsStmt = "select node_id from tree where node_id in (select parent_id from path where node_id = ?)"

main = withConn (\cn -> quickQuery' cn "select * from path" [])

conn = connectPostgreSQL "host=192.168.99.100 dbname=hello user=postgres"

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
    rs <- quickQuery' conn queryParentsStmt [seid who]
    let ps = map sqlid2Id . concat $ rs
    return ps

queryParent who = withConn (queryParent' who)

join' who dep conn = do
    let rec = [seid who, st2i who, seid dep, st2i dep]
        sql = joinPath who dep
    run conn "INSERT INTO TREE VALUES (?,?,?,?)" rec
    if null sql
        then commit conn
        else do
                run conn sql []
                commit conn

join who dep = withConn (join' who dep)

genChildren (Department n) cnt = map (Department . (n*10+)) [0..cnt-1]

itree' level ccnt parent@(Department pid) conn
    | pid > 10^level = return ()
    | otherwise = do
        let cs = genChildren parent ccnt
        new' cs conn
        mapM_ (\c -> join' c parent conn) cs
        mapM_ (\c -> itree' level ccnt c conn) cs
        return ()

createTree' level ccnt conn = do
   let root = Department 1
   new' [root] conn
   join' root root conn
   itree' level ccnt root conn

createTree level ccnt  = withConn (createTree' level ccnt)

sqlid2Id :: SqlValue -> Integer
sqlid2Id sid = read . filter (`elem` ['0'..'9']) $ s :: Integer
    where s =  fromSql sid :: String

clear' conn = do
    mapM_ (\stmt -> run conn stmt [])  [ "delete from path"
                                       , "delete from tree"
                                       , "delete from department"
                                       , "delete from member"
                                       ]
    commit conn

clear = withConn clear'
