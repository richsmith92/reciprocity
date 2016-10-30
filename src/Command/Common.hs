{-# LANGUAGE BangPatterns #-}

module Command.Common (module Command.Common, module Options.Applicative) where

import           CustomPrelude
import           Options.Applicative hiding ((<>))
import qualified  Language.Haskell.Interpreter as Hint
import           Data.ByteString.Internal (c2w, w2c)
import           Data.Text                (replace)
import Data.Conduit.Zlib (gzip)
import System.IO (IOMode(..), withBinaryFile)
import System.FilePath (splitExtension, addExtension)
import Data.Conduit.Zlib (ungzip)
import qualified Data.ByteString.Char8 as BC
import qualified Data.Conduit.Binary as CB
import Control.Monad.Trans.Maybe (MaybeT(..))

import           Data.Conduit.Internal        (ConduitM (..), Pipe (..))
import qualified Data.Conduit.Internal        as CI
import qualified Data.Vector as V
--
-- class (Show a) => ToRec a where
--   toRec :: a -> ByteString
--   toRec = encodeUtf8 . tshow
-- instance ToRecs ByteString where
--   toRecs = (:[])

data CmdInfo c = CmdInfo {
  cmdDesc :: Text,
  cmdParser :: Parser c
  }

class IsCommand c where
  runCommand :: Opts -> c -> IO ()
  commandInfo :: CmdInfo c

data Command = forall a. (Show a, IsCommand a) => Command a
deriving instance Show Command

data Opts = Opts {
  optsSep :: Text,
  optsHeader :: Bool,
  optsReplaceStr :: Text,
  optsKey :: SubRec,
  optsInputs :: [FilePath]
  } deriving (Show)

-- data SubRec = Fields [Natural] | FullRec
  -- deriving (Show)
--
-- data Key = Key {
--   keySubRec :: SubRec,
--   keyFun :: Text
-- } deriving Show

type StringLike a = (IsString a, IOData a, IsSequence a, Eq (Element a), Typeable a)

type SubRec = [Pair Natural]

execKey :: StringLike a => Opts -> a -> a
execKey opts = execSubRec opts (optsKey opts)

execSubRec :: (StringLike a) => Opts -> SubRec -> a -> a
execSubRec Opts{..} = \case
  [] -> id
  [rg] -> intercalate sep . subrec (over both fromIntegral rg) . splitSeq sep
  where
  subrec (i, j) = take (j - i + 1) . drop (i - 1)
  sep = fromString (unpack optsSep)

-- * Producers

inputSource :: (MonadResource m) => FilePath -> Source m ByteString
inputSource file = source .| lineChunksC
-- inputSource file = source .| CB.lines
  where
  source = if
    | file == "-" -> stdinC
    | ".gz" `isSuffixOf` file -> sourceFile file .| ungzip
    | otherwise -> sourceFile file

inputSources :: MonadResource m => Opts -> [Source m ByteString]
inputSources Opts{..} = case optsInputs of
  [] -> [stdinC .| lineChunksC]
  inputs -> map inputSource inputs

inputFiles :: Opts -> [FilePath]
inputFiles Opts{..} = case optsInputs of
  [] -> [""]
  inputs -> replaceElem "-" "" inputs

withInputSourcesH :: MonadResource m
  => Opts -> ([Source m ByteString] -> Source m ByteString) -> Source m ByteString
withInputSourcesH opts@Opts{..} combine = if
  | optsHeader, (source1:sources@(_:_)) <- lineSources -> do
    (resumable, header) <- lift $ source1 $$+ headC
    yieldMany header
    (tail1, finalize) <- lift $ unwrapResumable resumable
    combine $ tail1 : map (.| tailC) sources
    lift finalize
  | otherwise -> combine lineSources
  where
  lineSources = map (.| linesC) $ inputSources opts

catInputSources :: MonadResource m => Opts -> Source m ByteString
catInputSources opts@Opts{..} = if
  | optsHeader, (source1:sources@(_:_)) <- inputSources opts -> source1 >> mapM_ (.| tailC) sources
  | otherwise -> sequence_ $ inputSources opts

catHFiles :: (MonadResource m) => [FilePath] -> Source m ByteString
catHFiles = \case
  [] -> return ()
  (file:files) -> inputSource file >> mapM_ sourceDropHeader files

sourceDropHeader :: (MonadResource m) => FilePath -> Source m ByteString
sourceDropHeader file = inputSource file .| tailC

joinLists :: forall a k b. (Ord k) => (a -> k) -> (a -> k) ->  ([k] -> b) -> [a] -> [a] -> (Seq b, ([a], [a]))
joinLists key val combine xs ys = go (mempty :: Seq b) (map key xs) (map key ys) xs ys
  where
  go acc _ _ [] leftover = (acc, ([], leftover))
  go acc _ _ leftover [] = (acc, (leftover, []))
  go acc ks1@(k1:ks1') ks2@(k2:ks2') recs1@(r1:recs1') recs2@(r2:recs2') = case compare k1 k2 of
    LT -> go acc ks1' ks2 recs1' recs2
    GT -> go acc ks1 ks2' recs1 recs2'
    EQ -> go (acc `snoc` combine [k1, val r1, val r2]) ks1' ks2' recs1' recs2'

-- TEST: joinSources (CL.sourceList [1,3..10 :: Int]) (CL.sourceList [1..5]) $$ CL.mapM_ print
joinSources :: (Ord k, Monad m, StringLike a)
  => (a -> k) -> (a -> k) -> ([k] -> b) -> [Source m [a]] -> Producer m (Seq b)
joinSources key val combine [ConduitM !left0, ConduitM !right0] = ConduitM $ \rest -> let
  go (HaveOutput src1 close1 chunk1) (HaveOutput src2 close2 chunk2) = let
    (out, (lo1, lo2)) = joinLists key val combine chunk1 chunk2
    src1' = if null lo1 then src1 else CI.yield lo1 >> src1
    src2' = if null lo2 then src2 else CI.yield lo2 >> src2
    in HaveOutput (go src1' src2') (close1 >> close2) out
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
  in go (left0 Done) (right0 Done)

-- joinSourcesOn :: (Ord k, Monad m) => (a -> k) -> (a -> v) -> [Source m a] -> Producer m (k, [v])
-- joinSourcesOn key val = mergeResumable . fmap newResumableSource
--   where
--   mergeResumable sources = do
--     prefetchedSources <- lift $ traverse ($$++ await) sources
--     go $ fromList [keyRecSrc (r, s) | (s, Just r) <- prefetchedSources]
--   go v = do
--     let i = V.minIndexBy (comparing fst) v
--     let (kmin, (_, src1)) = v V.! i
--     let (kmax, _) = V.maximumBy (comparing fst) v
--     if | kmin /= kmax -> f src1 >>= maybe (return ()) (go . (\x -> v V.// [(i, keyRecSrc x)]))
--        | otherwise -> do
--          let (recs, srcs) = unzip $ map snd $ toList v
--          yield (kmin, map val recs)
--          runMaybeT (mapM (MaybeT . f) srcs) >>= maybe (return ()) (go . fromList . map keyRecSrc)
--   keyRecSrc (rec, src) = (key rec, (rec, src))
--   f src1 = do
--      (src2, mrec2) <- lift $ src1 $$++ await
--      return $ (,src2) <$> mrec2


-- * Transformers

linesC :: Monad m => Conduit ByteString m ByteString
linesC = CB.lines

linesCE :: Monad m => Conduit ByteString m [ByteString]
linesCE = mapC BC.lines

unlinesCE :: (Monad m, MonoFoldable mono, Element mono ~ ByteString) => Conduit mono m ByteString
unlinesCE = mapC (BC.unlines . toList)

lineChunksC :: Monad m => Conduit ByteString m ByteString
lineChunksC = await >>= maybe (return ()) go
  where
  go acc = await >>= maybe (yield acc) (go' . first (acc ++) . breakAfterEOL)
    where
    go' (x, y) = if null y then go x else yield x >> go y

tailC :: Monad m => Conduit a m a
tailC = await >>= maybe (return ()) (const $ awaitForever yield)

breakAfterEOL :: ByteString -> (ByteString, ByteString)
breakAfterEOL = second uncons . break (== 10) >>> \(x, m) -> maybe (x, "") (first (snoc x)) m

-- * Consumers

sinkMultiFile :: (IOData a, MonadIO m) => Sink (FilePath, a) m ()
sinkMultiFile = mapM_C $ \(file, line) ->  liftIO $
  withBinaryFile file AppendMode (`hPutStrLn` line)

splitSink :: (MonadIO m) => Opts -> FilePath -> Sink ByteString m ()
splitSink opts inFile = if optsHeader opts then await >>= sink else sink Nothing
  where
  sink (mheader :: Maybe ByteString) = foldMC (go mheader) (mempty :: Set FilePath) >> return ()
  fk = execKey opts
  (base, ext) = splitExtension inFile
  toFile line = base +? BC.unpack (fk line) +? ext
  x +? y = if null x then y else if null y then x else x ++ "." ++ y
  go mheader files line = liftIO $ uncurry (withBinaryFile file) $ if
    | file `member` files -> (AppendMode , \h -> hPutStrLn h line >> return files)
    | otherwise -> (WriteMode,
      \h -> traverse_ (hPutStrLn h) mheader >> hPutStrLn h line >> return (insertSet file files))
    where
    file = toFile line

stdoutSink :: (MonadIO m) => Consumer ByteString m ()
stdoutSink = unlinesAsciiC .| stdoutC

-- * Option parsing

type OptParser a = Mod OptionFields a -> Parser a
textOpt :: Textual s => (s -> a) -> OptParser a
textOpt parse = option (parse . pack <$> str)

natOpt :: OptParser Natural
natOpt mods = option auto (mods ++ metavar "N")

natRgOpt :: OptParser (Pair Natural)
natRgOpt mods = textOpt (parse . map read . splitSeq ("," :: Text)) (mods ++ metavar "FROM[,TO]")
  where
  parse = \case
    [i] -> (i, i)
    [i, j] -> if i <= j then (i, j) else error "natRgOpt: FROM > TO"
    _ -> error "natRgOpt: unrecognized format"

keyOpt :: Parser SubRec
keyOpt = many (natRgOpt (long "key" ++ short 'k' ++ help "Key subrecord"))

valueOpt :: Parser SubRec
valueOpt = many (natRgOpt (long "val" ++ help "Value subrecord"))

read s = fromMaybe (error $ "read: " ++ unpack s) $ readMay s

funOpt :: Mod OptionFields Text -> Parser Text
funOpt mods = textOpt id (mods ++ metavar "FUN" ++ value "")
