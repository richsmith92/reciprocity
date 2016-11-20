{-# LANGUAGE BangPatterns #-}
module Reciprocity.Base where

import CustomPrelude
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import Prelude ((!!))
import Reciprocity.Internal

data Opts = Opts {
  optsSep        :: Text,
  optsHeader     :: Bool,
  optsInputs     :: [FilePath]
  } deriving (Show)

defaultOpts :: Opts
defaultOpts = Opts {
  optsSep = "\t",
  optsHeader = False,
  optsInputs = []
  }

data Env s = Env {
  envOpts :: Opts,
  envSplit :: s -> [s],
  envIntercalate :: [s] -> s,
  envSep :: s
  }

-- type StringLike a = (IsString a, IOData a, IsSequence a, Eq (Element a), Typeable a)
type StringLike s = (s ~ ByteString)

type Subrec = [Pair (Maybe Natural)]

getEnv :: Opts -> Env ByteString
getEnv opts@Opts{..} = Env {
  envOpts = opts,
  envSplit = if
    | Just (c, "") <- uncons sep -> splitElemEx c
    | otherwise -> splitSeq sep,
  envIntercalate = intercalate sep,
  envSep = sep
  }
  where
  sep = fromString (unpack optsSep)

{-# INLINE getSubrec #-}
getSubrec :: (StringLike s) => Env s -> Subrec -> s -> s
getSubrec env sub = runIdentity . (subrec env) sub Identity

{-# INLINE subrec #-}
subrec :: (StringLike s) => Env s -> Subrec -> Lens' s s
subrec env = \case
  [] -> id
  [r] -> \f -> f . subrec' env (bounds r)
  _ -> error "subrec: multiple ranges not implemented"

{-# INLINE subrec' #-}
subrec' Env{..} (start, end) = case toList envSep of
  [c] -> if
    -- | start == 0, end == 0 -> takeWhile (/= c)
    | otherwise -> {-# SCC c_subrec_ #-} c_subrec c (start, end)
    -- | otherwise -> \s -> let v = byteStringToVector; is = B.elemIndices c s in slice (start, end) is (length is) s
  _ -> envIntercalate . take (end - start) . drop start . envSplit

bounds = (maybe 0 fromIntegral *** maybe maxBound fromIntegral)

-- {-# INLINE (!.) #-}
-- (!.) :: (Ord (Index seq), Num (Index seq), IsSequence seq) => seq -> Index seq -> Maybe (Element seq)
-- (!.) seq' idx = if idx < 0 then Nothing else headMay (drop idx seq')
--
-- {-# INLINE slice #-}
-- slice (start, end) is n s = let
--   i1 = {-# SCC i1 #-} if start > n then maxBound else (is !! (start - 1)) + 1
--   in {-# SCC sliceTakeDrop #-}
--   (if end >= n then id else B.take $ (is !! end) - i1) $ (if start == 0 then id else B.drop $ i1) s

{-# INLINE getKeyValue #-}
getKeyValue :: (StringLike s) => Env s -> Subrec -> Subrec -> s -> (s, s)
getKeyValue env@Env{..} sub1 sub2 = {-# SCC getKeyValue #-} if
  | otherwise -> getSubrec env sub1 &&& getSubrec env sub2

splitElemEx :: (IsSequence seq, Eq (Element seq)) => Element seq -> seq -> [seq]
splitElemEx x = splitWhen (== x)
{-# INLINE [0] splitElemEx #-}
{-# RULES "strict ByteString splitElemEx" splitElemEx = B.split #-}