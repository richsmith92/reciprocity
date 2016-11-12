module Lib.Command.Builtin where

import CustomPrelude
import Lib.Base
import Lib.Command.Base
-- import Lib.Hint
-- import Lib.Compile
import Data.Char (isSpace)

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

data CmdMerge = CmdMerge { mergeKey :: SubRec } deriving (Show)

instance IsCommand CmdMerge where
  commandInfo = CmdInfo {
    cmdDesc = "Merge ordered inputs into ordered output",
    cmdParser = do
      mergeKey <- keyOpt
      pure CmdMerge{..}
    }
  runCommand opts@Opts{..} CmdMerge{..} = runConduitRes $ withInputSourcesH opts $
    \header sources -> (yieldMany header >> merge (map (.| linesC) sources) .| unlinesAsciiC) .| stdoutC
    where
    merge = case mergeKey of
      [] -> mergeSourcesOn id
      _  -> mergeSourcesOn $ execSubRec opts mergeKey

-- * Join

data CmdJoin = CmdJoin {
  cmdJoinKey :: SubRec,
  cmdJoinValue :: SubRec,
  cmdJoinOuterLeft :: Bool,
  cmdJoinOuterRight :: Bool
  } deriving (Show)

instance IsCommand CmdJoin where

  commandInfo = CmdInfo {
    cmdDesc = "Join ordered headerless inputs on common key",
    cmdParser = do
      cmdJoinKey <- keyOpt
      cmdJoinValue <- valueOpt
      cmdJoinOuterLeft <- switch (short '1' ++ help "Outer join on first input")
      cmdJoinOuterRight <- switch (short '2' ++ help "Outer join on second input")
      return (CmdJoin{..})
    }

  runCommand opts@Opts{..} CmdJoin{..} = do
    let [s1, s2] = inputSources opts
    runConduitRes $ joinCE joinOpts (map (.| linesCE) [s1, s2]) .| unlinesCE .| stdoutC
    where
    {-# INLINE joinOpts #-}
    joinOpts = JoinOpts {
      joinOuterLeft = cmdJoinOuterLeft,
      joinOuterRight = cmdJoinOuterRight,
      joinKey = execSubRec opts cmdJoinKey,
      joinValue = execSubRec opts cmdJoinValue,
      joinCombine = case cmdJoinKey of
        [] -> headEx
        _  -> \[k,v1,v2] -> k ++ sep ++ v1 ++ sep ++ v2
      }
    sep = fromString (unpack optsSep)

-- * Split

data CmdSplit = CmdSplit {
  splitKey :: SubRec,
  splitPartition :: Bool,
  splitBuckets  :: Natural,
  splitTemplate :: Text
  } deriving (Show)
instance IsCommand CmdSplit where

  commandInfo = CmdInfo {
    cmdDesc = "Split into multiple files: put records having key KEY into file INPUT.KEY",
    cmdParser = do
      splitKey <- keyOpt
      splitPartition <- switch (long "partition" ++
        help "MapReduce partition mode: split into buckets determined by hash of the key")
      splitBuckets <- natOpt
        (long "buckets" ++ help "Number of buckets" ++ value 1)
      splitTemplate <- textOpt id (long "out" ++ value "{s}.{filename}" ++ help "Output filepath template (appended to input filename)")
      return CmdSplit{..}
    }

  runCommand opts (CmdSplit{..}) = sequence_ $ zipWith split (inputFiles opts) (inputSources opts)
    where
    split file source = runConduitRes $ withHeader opts source $ \h -> linesCE .| splitCE (toFile file) h
    fk = execSubRec opts splitKey
    toFile inFile = \rec -> unpack $ replace "{s}" (value rec) template
      where
      template = replace "{filename}" (pack $ takeFileName inFile) splitTemplate
      value = if
        | splitPartition -> tshow . bucket (fromIntegral splitBuckets) . fk
        | otherwise -> omap (\c -> if isSpace c then '.' else c) .decodeUtf8 . fk
    -- let (base, ext) = splitExtension inFile in base +? BC.unpack (fk rec) +? ext
    -- x +? y = if null x then y else if null y then x else x ++ "." ++ y

-- * Replace

data CmdReplace = CmdReplace {
  replaceDictFile :: FilePath,
  replaceDictKey :: SubRec,
  replaceDictValue :: SubRec
  }
