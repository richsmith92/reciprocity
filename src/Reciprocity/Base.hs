{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Reciprocity.Base where

import ReciprocityPrelude
import qualified Data.ByteString as B
-- import qualified Data.ByteString.Unsafe as B
-- import Reciprocity.Internal

data Opts = Opts {
  optsSep        :: Text,
  optsHeader     :: Bool,
  optsInputs     :: [FilePath],
  optsSubrec     :: Subrec
  } deriving (Show)

defaultOpts :: Opts
defaultOpts = Opts {
  optsSep = "\t",
  optsHeader = False,
  optsInputs = [],
  optsSubrec = []
  }

data Env s = Env {
  envOpts :: Opts,
  envSplit :: s -> [s],
  envIntercalate :: [s] -> s,
  envSep :: s
  }

-- type StringLike a = (IsString a, IOData a, IsSequence a, Eq (Element a), Typeable a)
type StringLike s = (s ~ ByteString)

-- | String without newline
newtype LineString s = LineString { unLineString :: s } deriving (Show, Eq, Ord, Monoid, Generic)
type LineBS = LineString ByteString

instance (t ~ LineString a') => Rewrapped (LineString a) t
instance Wrapped (LineString s) where
  type Unwrapped (LineString s) = s
  _Wrapped' = iso unLineString LineString
  {-# INLINE _Wrapped' #-}
instance (Hashable a) => Hashable (LineString a)

type FieldRange = Pair (Maybe Natural)
type Subrec = [FieldRange]

singleField :: Natural -> Subrec
singleField i = [dupe (Just i)]

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

bounds :: (Num c, Integral a, Bounded c) => (Maybe a, Maybe a) -> (c, c)
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

splitElemEx :: (IsSequence seq, Eq (Element seq)) => Element seq -> seq -> [seq]
splitElemEx x = splitWhen (== x)
{-# INLINE [0] splitElemEx #-}
{-# RULES "strict ByteString splitElemEx" splitElemEx = B.split #-}
