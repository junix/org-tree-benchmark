module Main where

import Database.HDBC
import Database.HDBC.PostgreSQL

data Entity = Department Int | SubCompany Int | Member Int deriving(Show,Eq,Read,Ord)

t2i :: Entity -> Int
t2i (Member _)     = 0
t2i (Department _) = 1
t2i (SubCompany _) = 2

eid :: Entity -> String
eid (Member id')     = 'm':show id'
eid (Department id') = 'd':show id'
eid (SubCompany id') = 's':show id'

value :: Entity -> [SqlValue]
value e@(Member id') = [toSql . eid $ e, toSql id']
value e = [toSql . eid $ e, toSql . t2i $ e]

istmt (Member _)     = "INSERT INTO member VALUES (?, ?)"
istmt (Department _) = "INSERT INTO department VALUES (?, 1)"
istmt (SubCompany _) = "INSERT INTO department VALUES (?, 2)"

main = do
   cn <- conn
   dset <- quickQuery' cn "select * from path" []
   print dset
   disconnect cn

conn = connectPostgreSQL "host=192.168.99.100 dbname=hello user=postgres"

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

