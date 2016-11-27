{-# LANGUAGE BangPatterns #-}

module Reciprocity.Conduit where

import CustomPrelude
import Reciprocity.Base

import qualified Data.ByteString.Char8 as BC
import qualified Data.Conduit.Binary   as CB
import           Data.Conduit.Zlib     (ungzip)
import           System.IO             (IOMode (..), withBinaryFile)
-- import Control.Monad.Trans.Maybe (MaybeT(..))
-- import qualified Data.Vector as V

import           Data.Conduit.Internal (ConduitM (..), Pipe (..))
import qualified Data.Conduit.Internal as CI
import           Codec.Archive.Zip

-- * Producers

inputSource :: (MonadResource m, StringLike s) => FilePath -> Source m s
inputSource file = source
  where
  source = if
    | file == "-" -> stdinC
    | ".gz" `isSuffixOf` file -> sourceFile file .| ungzip
    | ".zip" `isSuffixOf` file -> join $ liftIO $ withArchive file $ do
        [entry] <- entryNames
        getSource entry
    | otherwise -> sourceFile file

inputSources :: (MonadResource m, s ~ ByteString) => Env s -> [Source m s]
inputSources Env{..} = case optsInputs envOpts of
  []     -> [stdinC]
  inputs -> map inputSource inputs

inputFiles :: Env s -> [FilePath]
inputFiles Env{..} = case optsInputs envOpts of
  []     -> [""]
  inputs -> replaceElem "-" "" inputs

withInputSourcesH :: (MonadResource m, MonadTrans t, Monad (t m), StringLike s) =>
  Env s -> (Maybe s -> [Source m s] -> t m b) -> t m b
withInputSourcesH env combine = if
  | optsHeader, (_:_) <- sources -> do
    (unzip -> (sources', finalize), header:_) <- lift $
      unzip <$> mapM (($$+ lineAsciiC foldC) >=> _1 unwrapResumable) sources
    x <- combine (maybeAddEOL header) sources'
    lift $ sequence_ finalize
    return x
  | otherwise -> combine Nothing sources
  where
  sources = inputSources env
  Opts{..} = envOpts env

maybeAddEOL :: ByteString -> Maybe ByteString
maybeAddEOL = nonempty Nothing (Just . (`snoc` 10))

withHeader :: MonadIO m =>
  Env t -> Source m ByteString -> (Maybe ByteString -> ConduitM ByteString c m r) -> ConduitM () c m r
withHeader Env{..} source f = if
  | optsHeader envOpts -> do
    ((source', finalize), header) <- lift $ (($$+ lineAsciiC foldC) >=> _1 unwrapResumable) source
    x <- source' .| f (maybeAddEOL header)
    lift $ finalize
    return x
  | otherwise -> source .| f Nothing

data JoinOpts rec sub out = JoinOpts {
  joinOuterLeft, joinOuterRight :: Bool,
  joinKey, joinValue :: rec -> sub,
--  joinKeyValue :: rec -> (sub,sub),
  joinCombine :: [sub] -> out
  }

-- | Join multiple ordered sources (currenly works only for two sources)
joinCE :: (Ord sub, Monoid sub, Monad m) => JoinOpts rec sub out -> [Source m [rec]] -> Source m (Seq out)
joinCE jopts@JoinOpts{..} [ConduitM !left0, ConduitM !right0] = ConduitM $ \rest -> let
-- joinCE jopts@JoinOpts{..} inputs = ConduitM $ \rest -> let
  go (HaveOutput src1 close1 chunk1) (HaveOutput src2 close2 chunk2) = let
    (out, (lo1, lo2)) = joinLists jopts chunk1 chunk2
    src1' = if null lo1 then src1 else CI.yield lo1 >> src1
    src2' = if null lo2 then src2 else CI.yield lo2 >> src2
    in HaveOutput (go src1' src2') (close1 >> close2) out
  go l (Done ()) = when joinOuterLeft (CI.mapOutput finishL l) >> rest ()
  go (Done ()) r = when joinOuterRight (CI.mapOutput finishR r) >> rest ()
  go (Leftover left ()) right = go left right
  go left (Leftover right ())  = go left right
  go (NeedInput _ c) right = go (c ()) right
  go left (NeedInput _ c) = go left (c ())
  go (PipeM mx) (PipeM my) = PipeM (liftM2 go mx my)
  go (PipeM mx) y@HaveOutput{} = PipeM (liftM (\x -> go x y) mx)
  go x@HaveOutput{} (PipeM my) = PipeM (liftM (go x) my)
  in go (left0 Done) (right0 Done)
  where
  finishL = map (\rec -> joinCombine [joinKey rec, joinValue rec, mempty]) . fromList
  finishR = map (\rec -> joinCombine [joinKey rec, mempty, joinValue rec]) . fromList
  -- [ConduitM !left0, ConduitM !right0] = map (mapOutput $ map joinKeyValue) inputs
  -- finishL = map (\(k,v) -> joinCombine [k, v, mempty]) . fromList
  -- finishR = map (\(k,v) -> joinCombine [k, mempty, v]) . fromList

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
linesCE = lineChunksC .| mapC BC.lines

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

-- replaceConduit :: HashMap ByteString ByteString -> Conduit ByteString m ByteString
-- dictReplaceCE :: (MapValue map ~ k, ContainerKey map ~ k, IsMap map, Functor f, Monad m) =>
  -- Bool -> map -> ASetter s t k k -> Conduit (f s) m (f t)
dictReplaceCE :: (MapValue map ~ k, ContainerKey map ~ k, IsMap map, Monad m) =>
  Bool -> map -> ALens s b k k -> Conduit [s] m [b]
dictReplaceCE delete dict subrec = if
  | delete -> mapC $ mapMaybe $ cloneLens subrec $ \x -> lookup x dict
  | otherwise -> mapCE $ cloneLens subrec %~ \x -> fromMaybe x $ lookup x dict

subrecFilterCE :: (IsSequence b, Monad m) => (a -> Bool) -> Getting a (Element b) a -> Conduit b m b
subrecFilterCE p subrec = mapC $ filter $ p . view subrec

-- * Consumers

sinkMultiFile :: (IOData a, MonadIO m) => Sink (FilePath, a) m ()
sinkMultiFile = mapM_C $ \(file, line) ->  liftIO $
  withBinaryFile file AppendMode (`hPutStrLn` line)

splitCE :: (MonadIO m) => (ByteString -> FilePath) -> Bool -> Maybe ByteString -> Sink [ByteString] m ()
splitCE toFile mkdir mheader = foldMCE go (mempty :: Set FilePath) >> return ()
  where
  go files rec = liftIO $ if
    | file `member` files -> files <$ withBinaryFile file AppendMode (\h -> hPutStrLn h rec)
    | otherwise -> insertSet file files <$ newfile file rec
    where
    file = toFile rec
  newfile = case mheader of
    Nothing -> \file rec -> do
      newdir file
      withBinaryFile file AppendMode $ \h -> hPutStrLn h rec
    Just header -> \file rec -> do
      newdir file
      withBinaryFile file WriteMode $ \h -> hPutStrLn h header >> hPutStrLn h rec
  newdir file = when mkdir $ createDirectoryIfMissing True (takeDirectory file)
-- -- works slower than `joinCE` above
-- joinCE :: (Ord k, Monad m) => (a -> k) -> (a -> v) -> [Source m a] -> Producer m (k, [v])
-- joinCE key val = mergeResumable . fmap newResumableSource
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

-- * Pure helpers

-- | Join two ordered lists; return joined output and leftovers
joinLists :: forall rec sub out. (Ord sub, Monoid sub) =>
  -- JoinOpts rec sub out -> [(sub,sub)] -> [(sub,sub)] -> (Seq out, ([(sub,sub)], [(sub,sub)]))
  JoinOpts rec sub out -> [rec] -> [rec] -> (Seq out, ([rec], [rec]))
joinLists JoinOpts{..} xs ys = go xs ys (mempty :: Seq _)
  where
  go [] rest = (, ([], rest))
  go rest [] = (, (rest, []))

  go (r1:recs1) (r2:recs2) = iter (joinKey r1) (joinKey r2) r1 r2 recs1 recs2
  go1 _ r rest [] = (, (r:rest, []))
  go1 !k1 r1 recs1 (r2:recs2) = iter k1 (joinKey r2) r1 r2 recs1 recs2
  go2 _ r [] rest = (, ([], r:rest))
  go2 !k2 r2 (r1:recs1) recs2 = iter (joinKey r1) k2 r1 r2 recs1 recs2
  {-# INLINE iter #-}
  iter k1 k2 r1 r2 recs1 recs2 = {-# SCC iter #-} case compare k1 k2 of
    LT -> {-# SCC lt #-} go2 k2 r2 recs1 recs2 .! appendL k1 v1
    GT -> {-# SCC gt #-} go1 k1 r1 recs1 recs2 .! appendR k2 v2
    EQ -> {-# SCC eq #-} go recs1 recs2 .! append k1 v1 v2
    where
    v1 = joinValue r1
    v2 = joinValue r2

  -- go (r1:recs1) (r2:recs2) = iter r1 r2 recs1 recs2
  -- -- go1 _ r rest [] = (, (r:rest, []))
  -- -- go1 !(k1, r1 recs1 (r2:recs2) = iter k1 ({-# SCC key #-} joinKey r2) r1 r2 recs1 recs2
  -- go2 r2 [] rest = (, ([], r2:rest))
  -- go2 r2 (r1:recs1) recs2 = iter r1 r2 recs1 recs2
  -- -- {-# INLINE iter #-}
  -- iter (k1,v1) (k2,v2) recs1 recs2 = {-# SCC iter #-} case compare k1 k2 of
  --   LT -> {-# SCC lt #-} go2 (k2,v2) recs1 recs2 . appendL k1 v1
  --   -- GT -> {-# SCC gt #-} go1 (k1,v1) recs1 recs2 . appendR k2 v2
  --   EQ -> {-# SCC eq #-} go recs1 recs2 . append k1 v1 v2
  append k v1 v2 = (`snoc` {-# SCC "combine" #-} joinCombine [k, v1, v2])
  appendL = if joinOuterLeft then \k v -> append k v mempty else \_ _ -> id
  appendR = if joinOuterRight then \k v -> append k mempty v else \_ _ -> id
  {-# INLINE append #-}

(.!) :: (b -> c) -> (a -> b) -> a -> c
(.!) = (.) . ($!)
{-# INLINE (.!) #-}
infixr 9 .!

bucket :: Int -> ByteString -> Int
bucket n s = 1 + hash s `mod` n
