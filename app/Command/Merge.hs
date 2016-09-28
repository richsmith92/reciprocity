module Command.Merge where

import           ClassyPrelude.Conduit hiding ((<.>))
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)

import           Data.ByteString.Internal (c2w, w2c)
import Data.Conduit.Zlib (ungzip)
import Data.Conduit.Merge

mergeParser = do
  key <- keyOpt
  files <- many $ strArgument (metavar "INPUT.gz")
  return (CmdMerge key files)

data CmdMerge = CmdMerge Key [FilePath]
  deriving (Show)
instance IsCommand CmdMerge where
  runCommand opts (CmdMerge key files) = do
    fk <- execKey opts key
    runResourceT $
      mergeSourcesOn fk [sourceFile f $= ungzip $= linesUnboundedAsciiC | f <- files] $=
      unlinesAsciiC $$ stdoutC
