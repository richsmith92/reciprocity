module Lib.Base where

import CustomPrelude

data Opts = Opts {
  optsSep        :: Text,
  optsHeader     :: Bool,
  optsReplaceStr :: Text,
  optsKey        :: SubRec,
  optsInputs     :: [FilePath]
  } deriving (Show)

type StringLike a = (IsString a, IOData a, IsSequence a, Eq (Element a), Typeable a)

type SubRec = [Pair Natural]

execKey :: StringLike a => Opts -> a -> a
execKey opts = execSubRec opts (optsKey opts)

execSubRec :: (StringLike a) => Opts -> SubRec -> a -> a
execSubRec Opts{..} = \case
  [] -> id
  [rg] -> intercalate sep . subrec (over both fromIntegral rg) . splitSeq sep
  where
  subrec (i, j) = take (j - i + 1) . drop (i - 1)
  sep = fromString (unpack optsSep)
