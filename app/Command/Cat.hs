module Command.Cat where

import           ClassyPrelude.Conduit hiding ((<.>))
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)

data CmdCat = CmdCat [FilePath]
  deriving (Show)
instance IsCommand CmdCat where
  runCommand _opts (CmdCat files) =
    runResourceT $ sourceMultiHeader files $= unlinesAsciiC $$ stdoutC

catParser :: Parser CmdCat
catParser = do
  files <- many $ strArgument (metavar "INPUT")
  return (CmdCat files)
