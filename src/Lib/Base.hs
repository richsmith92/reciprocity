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

type SubRec = [Pair (Maybe Natural)]

{-# INLINE execKey #-}
execKey :: StringLike a => Opts -> a -> a
execKey opts = execSubRec opts (optsKey opts)

{-# INLINE execSubRec #-}
execSubRec :: (StringLike a) => Opts -> SubRec -> a -> a
execSubRec Opts{..} = \case
  [] -> id
  [rg] -> intercalate sep . subrec (over both (fmap fromIntegral) rg) . split
  _ -> error "subrec: multiple ranges not implemented"
  where
  split = if
    | Just (c, s) <- uncons sep, null s -> splitElemEx c
    | otherwise -> splitSeq sep
  subrec = \case
    (Just i, Just j) -> take (j - i + 1) . drop i
    (Just i, Nothing) -> drop i
    (Nothing, Just i) -> take (i + 1)
    (Nothing, Nothing) -> error "subrec: no fields"
  sep = fromString (unpack optsSep)

splitElemEx :: (IsSequence seq, Eq (Element seq)) => Element seq -> seq -> [seq]
splitElemEx x = splitWhen (== x)
{-# INLINE [0] splitElemEx #-}
{-# RULES "strict ByteString splitElemEx" splitElemEx = BS.split #-}
