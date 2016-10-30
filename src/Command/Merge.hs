module Command.Merge where

import           CustomPrelude
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)
import Data.Conduit.Merge

data CmdMerge = CmdMerge deriving (Show)

instance IsCommand CmdMerge where
  commandInfo = CmdInfo {
    cmdDesc = "Merge ordered inputs into ordered output",
    cmdParser = pure CmdMerge
    }
  runCommand opts@Opts{..} _ = runConduitRes $ withInputSourcesH opts merge .| stdoutSink
    where
    merge = case optsKey of
      [] -> mergeSources
      _ -> mergeSourcesOn $ execKey opts
