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
eid e@(Member _ id')     = soid e ++ 'm':show id'
eid e@(Department _ id') = soid e ++ 'd':show id'
eid e@(SubCompany _ id') = soid e ++ 's':show id'

oid :: Entity -> Int
oid (Member     org _) = org
oid (Department org _) = org
oid (SubCompany org _) = org

soid :: Entity -> String
soid = ('o':).show.oid
