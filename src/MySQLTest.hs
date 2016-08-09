module MySQLTest where
import Database.HDBC
import Database.HDBC.MySQL
import Data.List (intercalate)
import Data.Maybe


conn = connectMySQL defaultMySQLConnectInfo { mysqlHost = "127.0.0.1", mysqlUser = "jun", mysqlPassword = "" }
