{-# LANGUAGE BangPatterns #-}

module Lib.Conduit where

import CustomPrelude
import Lib.Base

import qualified Data.ByteString.Char8 as BC
import qualified Data.Conduit.Binary   as CB
import           Data.Conduit.Zlib     (ungzip)
import           System.IO             (IOMode (..), withBinaryFile)
-- import Control.Monad.Trans.Maybe (MaybeT(..))
-- import qualified Data.Vector as V

import           Data.Conduit.Internal (ConduitM (..), Pipe (..))
import qualified Data.Conduit.Internal as CI

-- * Producers

inputSource :: (MonadResource m) => FilePath -> Source m ByteString
inputSource file = source .| lineChunksC
  where
  source = if
    | file == "-" -> stdinC
    | ".gz" `isSuffixOf` file -> sourceFile file .| ungzip
    | otherwise -> sourceFile file

inputSources :: MonadResource m => Opts -> [Source m ByteString]
inputSources Opts{..} = case optsInputs of
  []     -> [stdinC .| lineChunksC]
  inputs -> map inputSource inputs

inputFiles :: Opts -> [FilePath]
inputFiles Opts{..} = case optsInputs of
  []     -> [""]
  inputs -> replaceElem "-" "" inputs

-- withInputSourcesH :: MonadResource m => Opts -> (Maybe ByteString -> [Source m ByteString] -> m b) -> m b
withInputSourcesH opts@Opts{..} combine = if
  | optsHeader, (_:_) <- sources -> do
    (unzip -> (sources', finalize), header:_) <- lift $
      unzip <$> mapM (($$+ lineAsciiC foldC) >=> _1 unwrapResumable) sources
    x <- combine (if null header then Nothing else Just header) sources'
    lift $ sequence_ finalize
    return x
  | otherwise -> combine Nothing sources
  where
  sources = inputSources opts

-- | Join two ordered lists; return joined output and leftovers
joinLists :: forall a k b. (Ord k) => (a -> k) -> (a -> k) ->  ([k] -> b) -> [a] -> [a] -> (Seq b, ([a], [a]))
joinLists key val combine xs ys = go (mempty :: Seq b) (map key xs) (map key ys) xs ys
  where
  go !acc _ _ [] leftover = (acc, ([], leftover))
  go !acc _ _ leftover [] = (acc, (leftover, []))
  go !acc ks1@(!k1:ks1') ks2@(!k2:ks2') recs1@(r1:recs1') recs2@(r2:recs2') = case compare k1 k2 of
    LT -> go acc ks1' ks2 recs1' recs2
    GT -> go acc ks1 ks2' recs1 recs2'
    EQ -> go (acc `snoc` combine [k1, val r1, val r2]) ks1' ks2' recs1' recs2'

-- | Join multiple ordered sources (currenly works only for two sources)
joinSources :: (Ord k, Monad m) => (a -> k) -> (a -> k) -> ([k] -> b) -> [Source m [a]] -> Producer m (Seq b)
joinSources key val combine [ConduitM !left0, ConduitM !right0] = ConduitM $ \rest -> let
  go (HaveOutput src1 close1 chunk1) (HaveOutput src2 close2 chunk2) = let
    (out, (lo1, lo2)) = joinLists key val combine chunk1 chunk2
    src1' = if null lo1 then src1 else CI.yield lo1 >> src1
    src2' = if null lo2 then src2 else CI.yield lo2 >> src2
    in HaveOutput (go src1' src2') (close1 >> close2) out
  -- go (Done ()) r = CI.mapOutput (fk &&& That . fv) r >> rest ()
  -- go l (Done ()) = CI.mapOutput (fk &&& This . fv) l >> rest ()
  go (Done ()) _ = rest ()
  go _ (Done ()) = rest ()
  go (Leftover left ()) right = go left right
  go left (Leftover right ())  = go left right
  go (NeedInput _ c) right = go (c ()) right
  go left (NeedInput _ c) = go left (c ())
  go (PipeM mx) (PipeM my) = PipeM (liftM2 go mx my)
  go (PipeM mx) y@HaveOutput{} = PipeM (liftM (\x -> go x y) mx)
  go x@HaveOutput{} (PipeM my) = PipeM (liftM (go x) my)
  in go (left0 Done) (right0 Done)

-- -- works slower than `joinSources` above
-- joinSources :: (Ord k, Monad m) => (a -> k) -> (a -> v) -> [Source m a] -> Producer m (k, [v])
-- joinSources key val = mergeResumable . fmap newResumableSource
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

-- | Merge multiple sorted sources into one sorted producer.

-- mergeSources :: (Ord a, Foldable f, Monad m) => f (Source m a) -> Producer m a
-- mergeSources = mergeSourcesOn id

-- | Merge multiple sorted sources into one sorted producer using specified sorting key.
mergeSourcesOn :: (Ord b, Monad m) => (a -> b) -> [Source m a] -> Producer m a
mergeSourcesOn key = mergeResumable . fmap newResumableSource . toList
  where
    mergeResumable sources = do
        prefetchedSources <- lift $ traverse ($$++ await) sources
        go [(a, s) | (s, Just a) <- prefetchedSources]
    go [] = pure ()
    go sources = do
        let (a, src1) : sources1 = sortOn (key . fst) sources
        yield a
        (src2, mb) <- lift $ src1 $$++ await
        let sources2 = case mb of
                Nothing -> sources1
                Just b  -> (b, src2) : sources1
        go sources2

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
  go acc = if
    | Just (_, 10) <- unsnoc acc -> yield acc >> lineChunksC
    | otherwise -> await >>= maybe (yield acc) (go' . breakAfterEOL)
    where
    go' (this, next) = let acc' = acc ++ this in if null next then go acc' else yield acc' >> go next

breakAfterEOL :: ByteString -> (ByteString, ByteString)
breakAfterEOL = second uncons . break (== 10) >>> \(x, m) -> maybe (x, "") (first (snoc x)) m

-- * Consumers

sinkMultiFile :: (IOData a, MonadIO m) => Sink (FilePath, a) m ()
sinkMultiFile = mapM_C $ \(file, line) ->  liftIO $
  withBinaryFile file AppendMode (`hPutStrLn` line)

splitSink :: (MonadIO m) => Opts -> (ByteString -> FilePath) -> Sink ByteString m ()
splitSink opts toFile = if optsHeader opts then await >>= sink else sink Nothing
  where
  sink (mheader :: Maybe ByteString) = foldMC (go mheader) (mempty :: Set FilePath) >> return ()
  go mheader files line = liftIO $ uncurry (withBinaryFile file) $ if
    | file `member` files -> (AppendMode , \h -> hPutStrLn h line >> return files)
    | otherwise -> (WriteMode,
      \h -> traverse_ (hPutStrLn h) mheader >> hPutStrLn h line >> return (insertSet file files))
    where
    file = toFile line
