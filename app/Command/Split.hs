module Command.Split where

import           ClassyPrelude.Conduit hiding ((<.>))
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)

data CmdSplit = CmdSplit
  deriving (Show)
instance IsCommand CmdSplit where
  runCommand opts (CmdSplit) = sequence_ $
    zipWith (\file source -> runConduitRes $ source .| linesC .| splitSink opts file)
      (inputFiles opts) (inputSources opts)

  commandInfo = CmdInfo {
    cmdDesc = "Split into multiple files: put records having key KEY into file INPUT.KEY",
    cmdParser = pure CmdSplit
    }
