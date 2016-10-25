module Command.Merge where

import           ClassyPrelude.Conduit hiding ((<.>))
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)

import           Data.ByteString.Internal (c2w, w2c)
import Data.Conduit.Zlib (ungzip)
import Data.Conduit.Merge

mergeParser = do
  key <- keyOpt
  files <- many $ strArgument (metavar "INPUT")
  return (CmdMerge key files)

data CmdMerge = CmdMerge Key [FilePath]
  deriving (Show)
instance IsCommand CmdMerge where
  runCommand opts (CmdMerge key files) = do
    merge <- case key of
      (Key FullRec "") -> return mergeSources
      _ -> mergeSourcesOn <$> execKey opts key
    runConduitRes $ merge (map sourceDecompress files) .| unlinesAsciiC .| stdoutC
