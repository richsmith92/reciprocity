module Command.Split where

import           ClassyPrelude.Conduit hiding ((<.>))
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)

data CmdSplit = CmdSplit SubRec [FilePath]
  deriving (Show)
instance IsCommand CmdSplit where
  runCommand = runSplit

runSplit :: Opts -> CmdSplit -> IO ()
runSplit opts (CmdSplit key files) = do
  forM_ files $ \inFile -> runResourceT $
    sourceFile inFile $= linesUnboundedAsciiC $= decodeUtf8C $$
    sinkMultiHeader (toFile inFile . fk)
    -- mapC (\s -> (fk $= unlinesC $$ stdoutC
  where
  fk = execSubRec opts key
  -- source = if null files then stdinC else mapM_ sourceFile files
  toFile inFile key = let (file, ext) = splitExtension inFile in
    file `addExtension` unpack key `addExtension` ext

splitParser :: Parser CmdSplit
splitParser = do
  key <- keyOpt
  files <- many $ strArgument (metavar "INPUT")
  return (CmdSplit key files)
