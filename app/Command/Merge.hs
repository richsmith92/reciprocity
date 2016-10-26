module Command.Merge where

import           CustomPrelude
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)
import Data.Conduit.Merge

mergeParser = do
  mergeKey <- keyOpt
  mergeFiles <- many $ strArgument (metavar "INPUT")
  return (CmdMerge{..})

data CmdMerge = CmdMerge {
  mergeKey :: SubRec,
  mergeFiles :: [FilePath]
  } deriving (Show)

instance IsCommand CmdMerge where
  runCommand opts (CmdMerge key files) = do
    runConduitRes $ merge (map sourceDecompress files) .| unlinesAsciiC .| stdoutC
    where
    merge = case key of
      [] -> mergeSources
      _ -> mergeSourcesOn $ execSubRec opts key
