module Main where
import Database.HDBC
--import Database.HDBC.PostgreSQL
import Database.HDBC.MySQL
import Data.List (intercalate)
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

joinPathSQL (Member _) dep = ""
joinPathSQL who dep = concat
    [ "INSERT INTO PATH "
    , "SELECT ", mkFields [quote cid, ctype, quote pid, ptype], " UNION "
    , "SELECT ", mkFields [quote cid, ctype, "PARENT_ID", "PARENT_TYPE"], " FROM PATH WHERE ", eqNodeExp pid ptype
    ]
    where cid   = eid who
          ctype = (show.t2i) who
          pid   = eid dep
          ptype = (show.t2i) dep

mkFields :: [String] -> String
mkFields = intercalate ","

eqNodeExp nid ntype =  " NODE_ID = " ++ quote nid ++ " AND NODE_TYPE = " ++ ntype
eqParentExp nid ntype =  " PARENT_ID = " ++ quote nid ++ " AND PARENT_TYPE = " ++ ntype

quote :: String -> String
quote s = '\'' : s ++ "'"

movPath (Member _) dep = ""
movPath who dep = concat
    [ "INSERT INTO PATH ( "
    , "VALUES (", mkFields [quote cid, ctype, quote pid, ptype], ") UNION "
    , "(SELECT ", mkFields [quote cid, ctype, "PARENT_ID", "PARENT_TYPE"] , " FROM PATH WHERE " , eqNodeExp pid ptype,   ") UNION "
    , "(SELECT ", mkFields ["NODE_ID", "NODE_TYPE", quote pid, ptype],      " FROM PATH WHERE " , eqParentExp cid ctype, ") UNION "
    , "(SELECT ", mkFields ["A.NODE_ID","A.NODE_TYPE", "B.PARENT_ID", "B.PARENT_TYPE"],  " FROM PATH A, PATH B WHERE "
    , "A.PARENT_ID = ", quote cid, " AND B.NODE_ID = " , quote pid
    , ")"
    ]
    where cid   = eid who
          ctype = (show.t2i) who
          pid   = eid dep
          ptype = (show.t2i) dep

querySubCntStmt (Department _) = "SELECT COUNT(*) FROM TREE WHERE NODE_ID IN " ++
                                 "(SELECT NODE_ID FROM PATH WHERE PARENT_ID = ? AND NODE_ID != PARENT_ID)"
queryParentsStmt = "SELECT NODE_ID FROM TREE WHERE NODE_ID IN (SELECT PARENT_ID FROM PATH WHERE NODE_ID = ?)"

main = withConn (\cn -> quickQuery' cn "select * from path" [])

--conn = connectPostgreSQL "host=192.168.99.100 dbname=hello user=postgres"
conn = connectMySQL defaultMySQLConnectInfo { mysqlHost = "127.0.0.1", mysqlUser = "jun", mysqlPassword = "" }


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
        sql = joinPathSQL who dep
    run conn "INSERT INTO TREE VALUES (?,?,?,?)" rec
    if null sql
        then commit conn
        else do
                run conn sql []
                commit conn

join who dep = withConn (join' who dep)

genChildren (Department n) cnt = map (Department . (n*10+)) [0..cnt-1]
genEmplee   (Department n) cnt = map (Member . (n*10+)) [0..cnt-1]

itree' level ccnt parent@(Department pid) conn
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

createTree' level ccnt conn = do
   let root = Department 1
   new' [root] conn
   join' root root conn
   itree' level ccnt root conn

createOrg level ccnt  = withConn (createTree' level ccnt)

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
