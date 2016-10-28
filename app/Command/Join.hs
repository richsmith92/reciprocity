module Command.Join where

import           CustomPrelude
import           Command.Common

import           System.FilePath       (addExtension, splitExtension)
import           Data.ByteString.Internal (c2w, w2c)
import           Data.These      (These (..), justThese)
import           Data.Conduit.Internal        (ConduitM (..), Pipe (..))
import qualified Data.Conduit.Internal        as CI

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

-- TEST: joinSources (CL.sourceList [1,3..10 :: Int]) (CL.sourceList [1..5]) $$ CL.mapM_ print
joinSources :: (Ord k, Monad m, StringLike a)
  => (a -> k) -> (a -> v) -> (k -> v -> v -> b) -> Source m a -> Source m a -> Source m b
joinSources fk fv combine (ConduitM left0) (ConduitM right0) = ConduitM $ \rest -> let
  -- go (Done ()) r = CI.mapOutput (fk &&& That . fv) r >> rest ()
  -- go l (Done ()) = CI.mapOutput (fk &&& This . fv) l >> rest ()
  go (Done ()) r = rest ()
  go l (Done ()) = rest ()
  go (Leftover left ()) right = go left right
  go left (Leftover right ())  = go left right
  go (NeedInput _ c) right = go (c ()) right
  go left (NeedInput _ c) = go left (c ())
  go (PipeM mx) (PipeM my) = PipeM (liftM2 go mx my)
  go (PipeM mx) y@HaveOutput{} = PipeM (liftM (\x -> go x y) mx)
  go x@HaveOutput{} (PipeM my) = PipeM (liftM (go x) my)
  go xs@(HaveOutput srcx closex s1) ys@(HaveOutput srcy closey s2) = let
    k1 = fk s1
    k2 = fk s2
    v1 = fv s1
    v2 = fv s2
    in case compare k1 k2 of
      -- LT -> HaveOutput (go srcx ys) closex $ (k1, This v1)
      -- GT -> HaveOutput (go xs srcy) closey $ (k2, That v2)
      LT -> go srcx ys
      GT -> go xs srcy
      EQ -> HaveOutput (go srcx srcy) (closex >> closey) (combine k1 v1 v2)
  in go (left0 Done) (right0 Done)
