module Main where

import Database.HDBC
import Database.HDBC.PostgreSQL

main = do
   cn <- conn
   dset <- quickQuery' cn "select * from path" []
   print dset
   disconnect cn

conn = connectPostgreSQL "host=192.168.99.100 dbname=hello user=postgres"

