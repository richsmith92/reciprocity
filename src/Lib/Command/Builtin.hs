module Lib.Command.Builtin where

import CustomPrelude
import Lib.Base
import Lib.Command.Base
-- import Lib.Hint
-- import Lib.Compile

-- import           System.Directory             (getHomeDirectory)
import Data.Text          (replace)
-- import Data.Conduit.Zlib  (gzip)
-- import Data.List.Extra    (groupSort)
-- import System.IO          (IOMode (..), withBinaryFile)
-- import System.Process

-- * Cat

data CmdCat = CmdCat deriving (Show)
instance IsCommand CmdCat where

  commandInfo = CmdInfo {
    cmdDesc = "Concatenate inputs",
    cmdParser = pure CmdCat
  }

  runCommand opts@Opts{..} _ = runConduitRes $ withInputSourcesH opts $
    \header sources -> (yieldMany header >> sequence_ sources) .| stdoutC

-- * Merge

data CmdMerge = CmdMerge deriving (Show)

instance IsCommand CmdMerge where
  commandInfo = CmdInfo {
    cmdDesc = "Merge ordered inputs into ordered output",
    cmdParser = pure CmdMerge
    }
  runCommand opts@Opts{..} _ = runConduitRes $ withInputSourcesH opts $
    \header sources -> (yieldMany header >> merge (map (.| linesC) sources) .| unlinesAsciiC) .| stdoutC
    where
    merge = case optsKey of
      [] -> mergeSourcesOn id
      _  -> mergeSourcesOn $ execKey opts

-- * Join

data CmdJoin = CmdJoin {
  joinValue :: SubRec
  } deriving (Show)

instance IsCommand CmdJoin where

  commandInfo = CmdInfo {
    cmdDesc = "Join ordered headerless inputs on common key",
    cmdParser = do
      joinValue <- valueOpt
      return (CmdJoin{..})
    }

  runCommand opts@Opts{..} CmdJoin{..} = do
    let [s1, s2] = inputSources opts
    runConduitRes $
      joinCE (execKey opts) (execSubRec opts joinValue) combine (map (.| linesCE) [s1, s2]) .|
      unlinesCE .| stdoutC
    where
    combine = case optsKey of
      [] -> headEx
      _  -> intercalate sep
    sep = fromString (unpack optsSep)

-- * Split

data CmdSplit = CmdSplit {
  splitPartition :: Bool,
  splitBuckets  :: Natural,
  splitTemplate :: Text
  } deriving (Show)
instance IsCommand CmdSplit where

  commandInfo = CmdInfo {
    cmdDesc = "Split into multiple files: put records having key KEY into file INPUT.KEY",
    cmdParser = do
      splitPartition <- switch (long "partition" ++
        help "Emulate map-reduce partition: split into buckets determined by hash of the key")
      splitBuckets <- natOpt
        (long "buckets" ++ help "Number of buckets" ++ value 1)
      splitTemplate <- textOpt id (value ("{s}" ++ ".{filename}") ++
        metavar "OUT_TEMPLATE" ++ help "Output filepath template (appended to input filename)")
      return CmdSplit{..}
    }

  runCommand opts (CmdSplit{..}) = sequence_ $ zipWith split (inputFiles opts) (inputSources opts)
    where
    split file source = runConduitRes $ withHeader opts source $ \h -> linesCE .| splitCE (toFile file) h
    fk = execKey opts
    toFile inFile = \rec -> unpack $ replace (optsReplaceStr opts) (value rec) template
      where
      template = replace "{filename}" (pack $ takeFileName inFile) splitTemplate
      value = if
        | splitPartition ->  tshow . bucket (fromIntegral splitBuckets) . fk
        | otherwise ->  decodeUtf8 . fk
    -- let (base, ext) = splitExtension inFile in base +? BC.unpack (fk rec) +? ext
    -- x +? y = if null x then y else if null y then x else x ++ "." ++ y

-- * Replace
