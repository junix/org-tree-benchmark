module Main where

import Database.HDBC
import Database.HDBC.PostgreSQL

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
joinPath who dep = concat $
    [ "INSERT INTO PATH (VALUES ('"++ nid, "',", sntype, ",'", pid, "',", sptype
    , ") UNION Select '", nid, "',", sntype
    , ", PARENT_ID, PARENT_TYPE FROM PATH WHERE NODE_ID = '", pid
    , "' AND NODE_TYPE = ", sptype, ")"
    ]
    where nid    = eid who
          sntype = (show.t2i) who
          pid    = eid dep
          sptype = (show.t2i) dep

main = do
   cn <- conn
   dset <- quickQuery' cn "select * from path" []
   print dset
   disconnect cn

conn = connectPostgreSQL "host=192.168.99.100 dbname=hello user=postgres"

exe :: String -> [SqlValue] -> Connection -> IO Integer
exe stmt args conn = run conn stmt args

newe :: Connection -> [Entity] -> IO [Integer]
newe conn [] = return []
newe conn es@(e:_) = do
    stmt <- prepare conn (istmt e)
    rs <- mapM (execute stmt) (map value es)
    commit conn
    return rs

neweIO :: [Entity] -> IO [Integer]
neweIO es = do
   cn <- conn
   rs <- newe cn es
   disconnect cn
   return rs

joinConn conn who dep = do
    let rec = [seid who, st2i who, seid dep, st2i dep]
        sql = joinPath who dep
    run conn "INSERT INTO TREE VALUES (?,?,?,?)" rec
    if null sql
        then commit conn
        else do
                run conn sql []
                commit conn

join who dep = do
   cn <- conn
   joinConn cn who dep
   disconnect cn

genChildren (Department n) cnt = map (Department . (n*10+)) [0..cnt-1]

itree conn level ccnt parent@(Department pid)
    | pid > 10^level = return ()
    | otherwise = do
        let cs = genChildren parent ccnt
        newe conn cs
        mapM (flip join parent) cs
        mapM (itree conn level ccnt) cs
        return ()

itreeIO level ccnt = do
   cn <- conn
   let root = Department 1
   newe cn [root]
   itree cn level ccnt root



