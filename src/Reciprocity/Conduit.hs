{-# LANGUAGE BangPatterns #-}

module Reciprocity.Conduit where

import ReciprocityPrelude
import Reciprocity.Base

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.Conduit.Binary   as CB
import           Data.Conduit.Zlib     (ungzip)
import           System.IO             (IOMode (..), withBinaryFile)
-- import Control.Monad.Trans.Maybe (MaybeT(..))
-- import qualified Data.Vector as V
import           Data.These      (These (..))

import           Data.Conduit.Internal (ConduitM (..), Pipe (..))
import qualified Data.Conduit.Internal as CI
import           Codec.Archive.Zip
import qualified ByteString.TreeBuilder as Builder
import qualified Codec.Compression.GZip as GZ
import Path.IO (resolveFile')
import Path (setFileExtension, filename)
import ByteString.StrictBuilder (builderBytes, builderLength, bytes)

newtype LineString s = LineString { unLineString :: s } deriving (Show, Eq, Ord)
type LineBS = LineString ByteString

linesC :: Monad m => Conduit ByteString m LineBS
linesC = mapOutput LineString CB.lines

joinLinesC :: Monad m => Conduit LineBS m ByteString
joinLinesC = awaitForever (\s -> yield (unLineString s) >> yield "\n") .| bsBuilderC (32*1024)

unlinesBSC :: Monad m => Conduit LineBS m ByteString
unlinesBSC = mapC ((`snoc` c2w '\n') . unLineString)

splitTsvLine :: (Eq (Element b), IsSequence b, IsString b) => LineString b -> [b]
splitTsvLine = readTsv . unLineString

useHeader :: Monad m => (o -> ConduitM o o m ()) -> ConduitM o o m ()
useHeader c = awaitJust $ \h -> yield h >> c h

dropHeader :: Monad m => Conduit LineBS m LineBS
dropHeader = do
  awaitJust $ \_h -> return () -- when (not $ "id" `isPrefixOf` unLineString h) (leftover h)
  awaitForever yield

-- concatWithHeaders :: Monad m => [Conduit LineBS m LineBS] -> Conduit LineBS m LineBS
concatWithHeaders :: Monad m => [ConduitM a LineBS m ()] -> ConduitM a LineBS m ()
concatWithHeaders sources = forM_ (zip [0..] sources) $ \case
  (0, s) -> s
  (_, s) -> s .| dropHeader


produceZip :: Bool -> FilePath -> Source (ResourceT IO) ByteString -> IO [Text]
produceZip overwrite outFile source = do
  exists <- doesFileExist outFile
  if | exists && not overwrite ->
        return [pack $ "Output file exists, skipping: " ++ outFile]
     | otherwise -> do
        unless exists $ createDirectoryIfMissing True (takeDirectory outFile)
        path <- resolveFile' outFile
        entryName <- setFileExtension "txt" $ filename path
        entrySel <- mkEntrySelector entryName
        createArchive path $ sinkEntry Deflate source entrySel
        return []

-- * Producers

inputC :: MonadResource m => FilePath -> Source m ByteString
inputC inFile = case takeExtension inFile of
  ".zip" -> join $ liftIO $ do
    path <- resolveFile' inFile
    withArchive path $ do
      entries <- keys <$> getEntries
      if | [entry] <- entries -> getEntrySource entry
         | otherwise -> error $ "More than one entry in zip file: " ++ inFile
  ".gz" -> sourceFile inFile $= ungzip
  _ -> sourceFile inFile

inputSources :: (MonadResource m, s ~ ByteString) => Env s -> [Source m s]
inputSources Env{..} = case optsInputs envOpts of
  []     -> [stdinC]
  inputs -> map inputC inputs

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

-- withInputSources :: forall (m :: * -> *) b. MonadResource m =>
  -- Env ByteString -> ConduitM ByteString ByteString m b -> ConduitM () ByteString m b
withInputSources :: MonadResource m =>
    Env ByteString
    -> (ByteString -> ConduitM ByteString ByteString m b)
    -> ConduitM () ByteString m b
withInputSources env c = withInputSourcesH env $
  \(Just header) sources -> yield header >> sequence_ sources .| (c header)

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

-- | TODO: somehow unite with `joinCE`
mergeOrderedSources :: (Ord a, Monad m)
  => Source m (a, b) -> Source m (a, b) -> Source m (a, These b b)
mergeOrderedSources (ConduitM left0) (ConduitM right0) = ConduitM $ \rest -> let
  go (Done ()) r = CI.mapOutput (second That) r >> rest ()
  go l (Done ()) = CI.mapOutput (second This) l >> rest ()
  go (Leftover left ()) right = go left right
  go left (Leftover right ())  = go left right
  go (NeedInput _ c) right = go (c ()) right
  go left (NeedInput _ c) = go left (c ())
  go (PipeM mx) (PipeM my) = PipeM (liftM2 go mx my)
  go (PipeM mx) y@HaveOutput{} = PipeM (liftM (\x -> go x y) mx)
  go x@HaveOutput{} (PipeM my) = PipeM (liftM (go x) my)
  go xs@(HaveOutput srcx closex (k1, v1)) ys@(HaveOutput srcy closey (k2, v2)) =
    case compare k1 k2 of
      LT -> HaveOutput (go srcx ys) closex $ (k1, This v1)
      GT -> HaveOutput (go xs srcy) closey $ (k2, That v2)
      EQ -> HaveOutput (go srcx srcy) (closex >> closey) (k1, These v1 v2)
  in go (left0 Done) (right0 Done)

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

-- linesC :: Monad m => Conduit ByteString m ByteString
-- linesC = CB.lines

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

sinkMultiFile :: (MonadIO m) => Sink (FilePath, ByteString) m ()
sinkMultiFile = mapM_C $ \(file, line) ->  liftIO $
  withBinaryFile file AppendMode (`BC.hPutStrLn` line)

splitCE :: (MonadIO m, MonoFoldable mono, Element mono ~ ByteString) =>
  (ByteString -> FilePath) -> Bool -> Bool -> Maybe ByteString -> Sink mono m ()
splitCE toFile mkdir compress mheader =
   foldlCE (\m rec -> insertWith (flip (++)) (toFile rec) (builder rec) m) (asHashMap mempty) >>=
   liftIO . mapM_ writeRecs . mapToList
  where
  builder rec = Builder.byteString rec ++ Builder.asciiChar '\n'
  writeRecs = case mheader of
    Nothing -> \(file, b) -> writeBuilder file b
    Just header -> \(file, b) -> writeBuilder file (builder header ++ b)
  writeBuilder file b = newdir file >> BL.writeFile file (whenC compress gzip $ Builder.toLazyByteString b)
  gzip = GZ.compressWith $ GZ.defaultCompressParams { GZ.compressLevel = GZ.bestSpeed }

  -- go files rec = liftIO $ if
  --   | file `member` files -> files <$ withBinaryFile file AppendMode (\h -> hPutStrLn h rec)
  --   | otherwise -> insertSet file files <$ newfile file rec
  --   where
  --   file = toFile rec
  -- newfile = case mheader of
  --   Nothing -> \file rec -> do
  --     newdir file
  --     withBinaryFile file AppendMode $ \h -> hPutStrLn h rec
  --   Just header -> \file rec -> do
  --     newdir file
  --     withBinaryFile file WriteMode $ \h -> hPutStrLn h header >> hPutStrLn h rec
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


awaitJust :: Monad m => (i -> ConduitM i o m ()) -> ConduitM i o m ()
awaitJust f = await >>= maybe (return ()) f


bsBuilderC :: Monad m => Int -> ConduitM ByteString ByteString m ()
bsBuilderC size = loop mempty
  where
  loop accum = await >>= maybe (if builderLength accum == 0 then return () else yield (builderBytes accum)) (\x ->
    let accum' = accum ++ bytes x in if
      | builderLength accum' >= size -> yield (builderBytes accum') >> loop mempty
      | otherwise -> loop accum')

-- @cropRangeC f (a, b)@ filters a subset of values between @a@ and @b@ from ordered stream of values
cropRangeC :: (Monad m, Ord b) => (a -> b) -> (Maybe b, Maybe b) -> Conduit a m a
cropRangeC f = \case
  (Nothing, Nothing) -> mapC id
  (Nothing, Just end) -> go2 end
  (Just start, Nothing) -> go1 start (mapC id)
  (Just start, Just end) -> go1 start (go2 end)
  where
  go1 start next = loop
    where loop = awaitJust $ \row -> let x = f row in if x >= start then leftover row >> next else loop
  go2 end = loop
    where loop = awaitJust $ \row -> let x = f row in when (x <= end) (yield row >> loop)

-- newtype FieldName = FieldName (unFieldName :: Text) deriving (Show, Eq, Ord)

data ColumnParams col = ColumnParams {
  colInputName :: String, -- ^ Input filename to be printed in error messages
  colNameFilter :: col -> Bool  -- ^ Which column names to include
  }

data ColumnInfo col = ColumnInfo {
  colHeader :: ByteString, -- ^ whole header row, unprocessed
  colNames :: [col] -- ^ Filtered column names, to be zipped with values
  }

withColumns :: Monad m => m (t, ResumableSource m a) -> (t -> Sink a m b) -> m b
withColumns colSource toSink = do
  (colInfo, resum) <- colSource
  resum $$+- toSink colInfo

columnsSource :: Monad m
  => ColumnParams ByteString -> Source m LineBS
  -> m (ColumnInfo ByteString, ResumableSource m [ByteString]) -- ^ header line, column names and columns resumable source
columnsSource ColumnParams{..} source = (source $$+ headC) >>= \case
    (resum, Nothing) -> closeResumableSource resum >>= error ("Empty file: " ++ colInputName)
    (resum, Just (LineString colHeader)) -> let
      (indices, colNames) = unzip [(i, col) | (i, col) <- zip [0..] allCols, colNameFilter col]
      filterCols = if
        | colNames == allCols -> id
        | otherwise -> getIndices indices
      allCols = readTsvBs colHeader
      in return (ColumnInfo{..}, resum $=+ mapC (filterCols . readTsvBs . unLineString))

getIndices :: [Int] -> [a] -> [a]
getIndices ks = go (\_ -> []) $ reverse diffs
  where
  diffs = take 1 ks ++ map (subtract 1) (zipWith (-) (drop 1 ks) ks)
  go f [] = f
  go f (i:is) = go ((\(x:xs) -> x : f xs) . drop i) is
