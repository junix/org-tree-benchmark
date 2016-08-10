module Org where

data Entity = Department Int Int
            | SubCompany Int Int
            | Member Int Int
            deriving(Show,Eq,Read,Ord)

t2i :: Entity -> Int
t2i (Member _ _)     = 0
t2i (Department _ _) = 1
t2i (SubCompany _ _) = 2


eid :: Entity -> String
eid (Member _ id')     = 'm':show id'
eid (Department _ id') = 'd':show id'
eid (SubCompany _ id') = 's':show id'

oid :: Entity -> Int
oid (Member     org _) = org
oid (Department org _) = org
oid (SubCompany org _) = org
