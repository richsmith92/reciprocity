module Reciprocity.Record where

import ReciprocityPrelude
-- import Reciprocity.Base

newtype LineString s = LineString { unLineString :: s } deriving (Show, Eq, Ord)
type LineBS = LineString ByteString

splitTsvLine :: (Eq (Element b), IsSequence b, IsString b) => LineString b -> [b]
splitTsvLine = readTsv . unLineString

joinTsvFields :: (IsSequence b, IsString b) => [b] -> LineString b
joinTsvFields = LineString . showTsv

-- Conduit utils

maybeAddEOL :: ByteString -> Maybe ByteString
maybeAddEOL = nonempty Nothing (Just . (`snoc` 10))

breakAfterEOL :: ByteString -> (ByteString, ByteString)
breakAfterEOL = second uncons . break (== 10) >>> \(x, m) -> maybe (x, "") (first (snoc x)) m

(.!) :: (b -> c) -> (a -> b) -> a -> c
(.!) = (.) . ($!)
{-# INLINE (.!) #-}
infixr 9 .!

bucket :: Int -> ByteString -> Int
bucket n s = 1 + hash s `mod` n

getIndices :: [Int] -> [a] -> [a]
getIndices ks = go (\_ -> []) $ reverse diffs
  where
  diffs = take 1 ks ++ map (subtract 1) (zipWith (-) (drop 1 ks) ks)
  go f [] = f
  go f (i:is) = go ((\(x:xs) -> x : f xs) . drop i) is
