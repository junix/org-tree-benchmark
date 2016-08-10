module SplitR where

-- range split
splitByStep step xs
    | length xs <= step = [xs]
    | otherwise = left : splitByStep step right
    where (left, right) = splitAt step xs

splitToCnt rs cnt = go rs cnt
    where step = length rs `quot` cnt
          go xs 1 = [xs]
          go xs n = left : go right (n-1)
             where (left, right) = splitAt step xs


