module Lib.Base where

import CustomPrelude
-- import qualified Data.Sequences as S
import qualified Data.ByteString as BS

data Opts = Opts {
  optsSep        :: Text,
  optsHeader     :: Bool,
  optsReplaceStr :: Text,
  optsKey        :: SubRec,
  optsInputs     :: [FilePath]
  } deriving (Show)

type StringLike a = (IsString a, IOData a, IsSequence a, Eq (Element a), Typeable a)

type SubRec = [Pair Natural]

{-# INLINE execKey #-}
execKey :: StringLike a => Opts -> a -> a
execKey opts = execSubRec opts (optsKey opts)

{-# INLINE execSubRec #-}
execSubRec :: (StringLike a) => Opts -> SubRec -> a -> a
execSubRec Opts{..} = \case
  [] -> id
  [rg] -> intercalate sep . subrec (over both fromIntegral rg) . split
  where
  split = if
    | Just (c, s) <- uncons sep, null s -> splitElemEx c
    | otherwise -> splitSeq sep
  subrec (i, j) = take (j - i + 1) . drop (i - 1)
  sep = fromString (unpack optsSep)

splitElemEx :: (IsSequence seq, Eq (Element seq)) => Element seq -> seq -> [seq]
splitElemEx x = splitWhen (== x)
{-# INLINE [0] splitElemEx #-}
{-# RULES "strict ByteString splitElemEx" splitElemEx = BS.split #-}
