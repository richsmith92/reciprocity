module Command.Join where

import           CustomPrelude
import           Command.Common
import           Data.Conduit.Internal        (ConduitM (..), Pipe (..))
import qualified Data.Conduit.Internal        as CI

data CmdJoin = CmdJoin {
  joinValue :: SubRec
  } deriving (Show)

instance IsCommand CmdJoin where
  runCommand opts@Opts{..} cmd@CmdJoin{..} = do
    let [s1, s2] = [s .| linesCE | s <- inputSources opts]
    -- runConduitRes $ joinSources (execKey opts) (execSubRec opts joinValue) [s1, s2] .|
    runConduitRes $ joinSources (execKey opts) (execSubRec opts joinValue) combine [s1, s2] .|
      unlinesCE .| stdoutC
    where
    combine = case optsKey of
      [] -> headEx
      _ -> intercalate sep
    sep = fromString (unpack optsSep)

  commandInfo = CmdInfo {
    cmdDesc = "Join ordered headerless inputs on common key",
    cmdParser = do
      joinValue <- valueOpt
      return (CmdJoin{..})
    }
