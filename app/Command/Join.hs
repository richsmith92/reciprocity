module Command.Join where

import           CustomPrelude
import           Command.Common

-- import           System.FilePath       (addExtension, splitExtension)
-- import           Data.ByteString.Internal (c2w, w2c)
-- import           Data.These      (These (..), justThese)

data CmdJoin = CmdJoin {
  joinValue :: SubRec
  } deriving (Show)

instance IsCommand CmdJoin where
  runCommand opts@Opts{..} cmd@CmdJoin{..} = do
    let [s1, s2] = inputSources opts
    runConduitRes $ joinSources (execKey opts) (execSubRec opts joinValue) combine s1 s2 .| stdoutSink
    where
    combine = case optsKey of
      [] -> \k _ _ -> k
      _ -> \k v1 v2 -> k ++ sep ++ v1 ++ sep ++ v2
    sep = fromString (unpack optsSep)

  commandInfo = CmdInfo {
    cmdDesc = "Join ordered headerless inputs on common key",
    cmdParser = do
      joinValue <- valueOpt
      return (CmdJoin{..})
    }
