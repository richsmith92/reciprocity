module Command.Join where

import           CustomPrelude
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)

import           Data.ByteString.Internal (c2w, w2c)
import           Data.These      (These (..), justThese)

import           Data.Conduit.Internal        (ConduitM (..), Pipe (..))
import qualified Data.Conduit.Internal        as CI

joinParser = do
  joinKey <- keyOpt
  joinValue <- valueOpt
  joinFiles <- many $ strArgument (metavar "INPUT")
  return (CmdJoin{..})

data CmdJoin = CmdJoin {
  joinKey :: SubRec,
  joinValue :: SubRec,
  joinFiles :: [FilePath]
  } deriving (Show)

instance IsCommand CmdJoin where
  runCommand opts@Opts{..} cmd@CmdJoin{..} = do
    let [s1, s2] = map sourceDecompress joinFiles
    runConduitRes $ joinSources opts cmd s1 s2 .| concatMapC (uncurry out) .| unlinesAsciiC .| stdoutC
    where
    out k = combine k <.> justThese
    combine = case joinKey of
      [] -> const
      _ -> \k (v1, v2) -> k ++ sep ++ v1 ++ sep ++ v2
    sep = fromString (unpack optsSep)

-- TEST: joinSources (CL.sourceList [1,3..10 :: Int]) (CL.sourceList [1..5]) $$ CL.mapM_ print
joinSources :: (Ord a, Monad m, StringLike a) => Opts -> CmdJoin -> Source m a -> Source m a -> Source m (a, These a a)
joinSources opts CmdJoin{..} (ConduitM left0) (ConduitM right0) = ConduitM $ \rest -> let
  go (Done ()) r = CI.mapOutput (fk &&& That . fv) r >> rest ()
  go l (Done ()) = CI.mapOutput (fk &&& This . fv) l >> rest ()
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
      LT -> HaveOutput (go srcx ys) closex $ (k1, This v1)
      GT -> HaveOutput (go xs srcy) closey $ (k2, That v2)
      EQ -> HaveOutput (go srcx srcy) (closex >> closey) (k1, These v1 v2)
  in go (left0 Done) (right0 Done)
  where
  fk = execSubRec opts joinKey
  fv = execSubRec opts joinValue
