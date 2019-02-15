module Reciprocity.Record (
  module Reciprocity.Record,
  module Reciprocity.Base
  ) where

import ReciprocityPrelude
import Reciprocity.Base
import Reciprocity.Internal

splitTsvLine :: (Eq (Element b), IsSequence b, IsString b) => LineString b -> [b]
splitTsvLine = readTsv . unLineString

joinTsvFields :: (IsSequence b, IsString b) => [b] -> LineString b
joinTsvFields = LineString . showTsv

-- | strip 'LineString' newtype and append EOL char
unLineStringEOL :: LineBS -> ByteString
unLineStringEOL = (`snoc` c2w '\n') . unLineString

maybeAddEOL :: ByteString -> Maybe ByteString
maybeAddEOL = nonempty Nothing (Just . (`snoc` c2w '\n'))

breakAfterEOL :: ByteString -> (ByteString, ByteString)
breakAfterEOL = second uncons . break (== c2w '\n') >>> \(x, m) -> maybe (x, "") (first (snoc x)) m

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

{-# INLINE getSubrec #-}
getSubrec :: (StringLike s) => Env s -> Subrec -> LineString s -> LineString s
getSubrec _env@Env{..} ranges = if
  | [] <- ranges -> id
  | [r] <- ranges, [sep] <- toList envSep -> LineString . getSub sep (bounds r) . unLineString
  | [sep] <- toList envSep -> LineString . getSubs sep . unLineString
  | otherwise -> error "Multibyte separators not implemented yet"
  where
  getSubs sep s = intercalate envSep $ [getSub sep (bounds r) s | r <- ranges]

{-# INLINE subrec #-}
subrec :: (StringLike s) => Env s -> Subrec -> Lens' LineBS LineBS
subrec Env{..} sub = if
  -- | [] <- sub -> id
  | [r] <- sub, [sep] <- toList envSep -> subLens sep (bounds r) -- . iso LineString unLineString
  -- | [r] <- sub, [sep] <- toList envSep -> iso unLineString LineString . subLens sep (bounds r)
  | otherwise -> error "Multiple ranges and multibyte separators not implemented yet"

-- {-# INLINE subrec' #-}
-- subrec' Env{..} (start, end) = case toList envSep of
--   [c] -> c_subrec c (start, end)
--     -- | start == 0, end == 0 -> takeWhile (/= c)
--   _ -> envIntercalate . take (end - start) . drop start . envSplit

{-# INLINE subLens #-}
subLens :: Word8 -> (Int, Int) -> Lens' LineBS LineBS
subLens sep bounds = \f (LineString str) -> let sub = getSub sep bounds str in
  LineString . (\sub' -> replaceSub sub sub' str) . unLineString <$> f (LineString sub)

{-# INLINE getKeyValue #-}
getKeyValue :: Env ByteString -> Subrec -> Subrec -> LineBS -> (LineBS, LineBS)
getKeyValue env@Env{..} sub1 sub2 = {-# SCC getKeyValue #-} if
  | otherwise -> getSubrec env sub1 &&& getSubrec env sub2
