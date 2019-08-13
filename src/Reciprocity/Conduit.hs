{-# LANGUAGE BangPatterns #-}

module Reciprocity.Conduit where

import ReciprocityPrelude
import Reciprocity.Base
import Reciprocity.Record

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.Conduit.Binary   as CB
import           Data.Conduit.Zlib     (ungzip)
import           System.IO             (IOMode (..), withBinaryFile)
import           Data.These      (These (..))

import           Data.Conduit.Internal (ConduitT (..), Pipe (..))
import qualified Data.Conduit.Internal as CI
import           Codec.Archive.Zip
import qualified ByteString.TreeBuilder as Builder
import qualified Codec.Compression.GZip as GZ
import ByteString.StrictBuilder (builderBytes, builderLength, bytes)

linesC :: Monad m => ConduitT ByteString LineBS m ()
linesC = mapOutput LineString CB.lines

mapLinesC :: (Functor f, Monad m) => f (ConduitT a ByteString m ()) -> f (ConduitT a LineBS m ())
mapLinesC = map (.| linesC)

joinLinesC :: Monad m => ConduitT LineBS ByteString m ()
joinLinesC = awaitForever (\s -> yield (unLineString s) >> yield "\n") .| bsBuilderC (32*1024)

unlinesBSC :: Monad m => ConduitT LineBS ByteString m ()
unlinesBSC = mapC ((`snoc` c2w '\n') . unLineString)

useHeader :: Monad m => (o -> ConduitT o o m ()) -> ConduitT o o m ()
useHeader c = awaitJust $ \h -> yield h >> c h

dropHeader :: Monad m => ConduitT LineBS LineBS m ()
dropHeader = do
  awaitJust $ \_h -> return ()
  awaitForever yield

concatWithHeaders :: Monad m => [ConduitT a LineBS m ()] -> ConduitT a LineBS m ()
concatWithHeaders sources = sequence_ $ case uncons sources of
  Nothing -> []
  Just (source1, sources') -> source1 : map (.| dropHeader) sources'

produceZip :: Bool -> FilePath -> ConduitT () ByteString (ResourceT IO) () -> IO [Text]
produceZip overwrite outFile source = do
  exists <- doesFileExist outFile
  if | exists && not overwrite ->
        return [pack $ "Output file exists, skipping: " ++ outFile]
     | otherwise -> do
        unless exists $ createDirectoryIfMissing True (takeDirectory outFile)
        entrySel <- mkEntrySelector $ takeBaseName outFile ++ ".txt"
        createArchive outFile $ sinkEntry Deflate source entrySel
        return []

-- * Producers

type MonadRec m = (MonadResource m, PrimMonad m, MonadThrow m)

inputC :: (MonadRec m) => FilePath -> ConduitT () ByteString m ()
inputC inFile = case takeExtension inFile of
  ".zip" -> join $ liftIO $ do
    -- path <- resolveFile' inFile
    withArchive inFile $ do
      entries <- keys <$> getEntries
      if | [entry] <- entries -> getEntrySource entry
         | otherwise -> error $ "More than one entry in zip file: " ++ inFile
  ".gz" -> sourceFile inFile .| ungzip
  _ -> sourceFile inFile

inputSources :: (MonadRec m, s ~ ByteString) => Env s -> [ConduitT () s m ()]
inputSources Env{..} = case optsInputs envOpts of
  []     -> [stdinC]
  inputs -> map inputC inputs

inputFiles :: Env s -> [FilePath]
inputFiles Env{..} = case optsInputs envOpts of
  []     -> [""]
  inputs -> replaceElem "-" "" inputs

-- | Yield all input lines, adding newline to the end of each.
yieldManyWithEOL :: (Element mono ~ LineBS, Monad m, MonoFoldable mono) =>
     mono -> ConduitT i ByteString m ()
yieldManyWithEOL = mapM_ (yield . unLineStringEOL)

-- | Apply given function to the header of the first input and sources for all input bodies.
-- Header will be either Nothing or Just string with newline.
withInputSourcesH :: (MonadRec m, MonadTrans t, Monad (t m), StringLike s) =>
  Env s -> ([(Maybe (LineString s), ConduitT () s m ())] -> t m b) -> t m b
withInputSourcesH env combine = if
  | optsHeader, (_:_) <- sources -> do
    (sources', headers) <- unzip <.> lift $
      -- take 1 line and return Source for the rest (not split by lines)
      forM sources $ \source -> first unsealConduitT <$> (source $$+ lineAsciiC foldC)
    let headerLine h = if null h then Nothing else Just (LineString h)
    let headerLines = map headerLine headers
    combine $ zip headerLines sources'
  | otherwise -> combine $ zip (repeat Nothing) sources
  where
  sources = inputSources env
  Opts{..} = envOpts env

withHeader :: MonadIO m
  => Env t -> ConduitT () ByteString m ()
  -> (Maybe LineBS -> ConduitT ByteString c m r) -> ConduitT () c m r
withHeader Env{..} source f = if
  | optsHeader envOpts -> do
    (source', header) <- lift $ first unsealConduitT <$> (source $$+ lineAsciiC foldC)
    source' .| f (nonempty Nothing (Just . LineString) header)
  | otherwise -> source .| f Nothing

data JoinOpts rec sub out = JoinOpts {
  joinOuterLeft, joinOuterRight :: Bool,
  joinKey, joinValue :: LineString rec -> LineString sub,
  joinCombine :: [LineString sub] -> out
  }

-- | Join multiple ordered sources (currenly works only for two sources)
joinCE :: (Ord sub, Monoid sub, Monad m)
  => JoinOpts rec sub out
  -> [ConduitT () [LineString rec] m ()]
  -> ConduitT () (Seq out) m ()
joinCE jopts@JoinOpts{..} [ConduitT !left0, ConduitT !right0] = ConduitT $ \rest -> let
-- joinCE jopts@JoinOpts{..} inputs = ConduitT $ \rest -> let
  go (HaveOutput src1 chunk1) (HaveOutput src2 chunk2) = let
    (out, (lo1, lo2)) = joinLists jopts chunk1 chunk2
    src1' = if null lo1 then src1 else CI.yield lo1 >> src1
    src2' = if null lo2 then src2 else CI.yield lo2 >> src2
    in HaveOutput (go src1' src2') out
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
joinCE _ _ = error "joinCE: Expecting exactly 2 conduits!"
  -- [ConduitT !left0, ConduitT !right0] = map (mapOutput $ map joinKeyValue) inputs
  -- finishL = map (\(k,v) -> joinCombine [k, v, mempty]) . fromList
  -- finishR = map (\(k,v) -> joinCombine [k, mempty, v]) . fromList

-- | TODO: somehow unite with `joinCE`
mergeOrderedSources :: (Ord a, Monad m)
  => ConduitT () (a, b) m () -> ConduitT () (a, b) m () -> ConduitT () (a, These b b) m ()
mergeOrderedSources (ConduitT left0) (ConduitT right0) = ConduitT $ \rest -> let
  go (Done ()) r = CI.mapOutput (second That) r >> rest ()
  go l (Done ()) = CI.mapOutput (second This) l >> rest ()
  go (Leftover left ()) right = go left right
  go left (Leftover right ())  = go left right
  go (NeedInput _ c) right = go (c ()) right
  go left (NeedInput _ c) = go left (c ())
  go (PipeM mx) (PipeM my) = PipeM (liftM2 go mx my)
  go (PipeM mx) y@HaveOutput{} = PipeM (liftM (\x -> go x y) mx)
  go x@HaveOutput{} (PipeM my) = PipeM (liftM (go x) my)
  go xs@(HaveOutput srcx (k1, v1)) ys@(HaveOutput srcy (k2, v2)) =
    case compare k1 k2 of
      LT -> HaveOutput (go srcx ys) (k1, This v1)
      GT -> HaveOutput (go xs srcy) (k2, That v2)
      EQ -> HaveOutput (go srcx srcy) (k1, These v1 v2)
  in go (left0 Done) (right0 Done)

-- | Merge multiple sorted sources into one sorted producer.

-- mergeSources :: (Ord a, Foldable f, Monad m) => f (Source m a) -> Producer m a
-- mergeSources = mergeSourcesOn id

-- | Merge multiple sorted sources into one sorted producer using specified sorting key.
mergeSourcesOn :: (Ord b, Monad m) => (a -> b) -> [ConduitT () a m ()] -> ConduitT () a m ()
mergeSourcesOn key = mergeResumable . fmap sealConduitT . toList
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

linesBS :: ByteString -> [LineBS]
linesBS = map LineString . BC.lines

unlinesBS :: [LineBS] -> ByteString
unlinesBS = BC.unlines . map unLineString

linesCE :: Monad m => ConduitT ByteString [LineBS] m ()
linesCE = mapOutput linesBS lineChunksC

unlinesCE :: (Monad m, MonoFoldable mono, Element mono ~ LineBS) => ConduitT mono ByteString m ()
unlinesCE = mapC (unlinesBS . toList)

lineChunksC :: Monad m => ConduitT ByteString ByteString m ()
lineChunksC = await >>= maybe (return ()) go
  where
  go acc = if
    | Just (_, 10) <- unsnoc acc -> yield acc >> lineChunksC
    | otherwise -> await >>= maybe (yield acc) (go' . breakAfterEOL)
    where
    go' (this, next) = let acc' = acc ++ this in if null next then go acc' else yield acc' >> go next

dictReplaceCE :: (MapValue map ~ k, ContainerKey map ~ k, IsMap map, Monad m) =>
  Bool -> map -> ALens s b k k -> ConduitT [s] [b] m ()
dictReplaceCE keep dict subrec = if
  | keep -> mapCE $ cloneLens subrec %~ \x -> fromMaybe x $ lookup x dict
  | otherwise -> mapC $ mapMaybe $ cloneLens subrec $ \x -> lookup x dict

subrecFilterCE :: (IsSequence b, Monad m) => (a -> Bool) -> Getting a (Element b) a -> ConduitT b b m ()
subrecFilterCE p subrec = mapC $ filter $ p . view subrec

-- * Consumers

sinkMultiFile :: (MonadIO m) => ConduitT (FilePath, ByteString) Void m ()
sinkMultiFile = mapM_C $ \(file, line) ->  liftIO $
  withBinaryFile file AppendMode (`BC.hPutStrLn` line)

splitCE :: (MonadIO m, MonoFoldable mono, Element mono ~ LineBS) =>
  (LineBS -> FilePath) -- ^ function to produce filepath
  -> Bool -- ^ run mkdir for each file?
  -> Bool -- ^ gzip output files?
  -> Maybe LineBS -- ^ Header for each output file
  -> ConduitT mono Void m ()
splitCE toFile mkdir compress mheader = do
  out <- foldlCE
    (\m rec -> insertWith (flip (++)) (toFile rec) (builder $ unLineString rec) m)
    (asHashMap mempty)
  liftIO $ mapM_ writeRecs $ mapToList out
  where
  builder rec = Builder.byteString rec ++ Builder.asciiChar '\n'
  writeRecs = case mheader of
    Nothing -> \(file, b) -> writeBuilder file b
    Just (LineString header) -> \(file, b) -> writeBuilder file (builder (header ++ "\n") ++ b)
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
  JoinOpts rec sub out -> [LineString rec] -> [LineString rec]
  -> (Seq out, ([LineString rec], [LineString rec]))
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


awaitJust :: Monad m => (i -> ConduitT i o m ()) -> ConduitT i o m ()
awaitJust f = await >>= maybe (return ()) f

bsBuilderC :: Monad m => Int -> ConduitM ByteString ByteString m ()
bsBuilderC size = loop mempty
  where
  loop accum = await >>= maybe (if builderLength accum == 0 then return () else yield (builderBytes accum)) (\x ->
    let accum' = accum ++ bytes x in if
      | builderLength accum' >= size -> yield (builderBytes accum') >> loop mempty
      | otherwise -> loop accum')

-- @cropRangeC f (a, b)@ filters a subset of values between @a@ and @b@ from ordered stream of values
cropRangeC :: (Monad m, Ord b) => (a -> b) -> (Maybe b, Maybe b) -> ConduitT a a m ()
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

withColumns :: Monad m => m (t, SealedConduitT () a m ()) -> (t -> ConduitT a Void m b) -> m b
withColumns colSource toSink = do
  (colInfo, resum) <- colSource
  resum $$+- toSink colInfo

columnsSource :: Monad m
  => ColumnParams ByteString -> ConduitT () LineBS m ()
  -> m (ColumnInfo ByteString, SealedConduitT () [ByteString] m ()) -- ^ header line, column names and columns resumable source
columnsSource ColumnParams{..} source = (source $$+ headC) >>= \case
    (_resum, Nothing) -> -- closeResumableSource resum >>=
      error ("Empty file: " ++ colInputName)
    (resum, Just (LineString colHeader)) -> let
      (indices, colNames) = unzip [(i, col) | (i, col) <- zip [0..] allCols, colNameFilter col]
      filterCols = if
        | colNames == allCols -> id
        | otherwise -> getIndices indices
      allCols = readTsvBs colHeader
      in return (ColumnInfo{..}, resum $=+ mapC (filterCols . readTsvBs . unLineString))

catWithHeaderC :: Monad m
  => Env ByteString -> [(Maybe LineBS, ConduitT () ByteString m ())] -> ConduitT () ByteString m ()
catWithHeaderC env@Env{..} headersSources = case optsSubrec envOpts of
  [] -> do
    yieldManyWithEOL header1
    sequence_ sources -- bodies not split in lines
  _ -> do
    yieldManyWithEOL $ preprocessHeader env header1
    sequence_ [preprocessC env h (s .| linesC) | (h, s) <- headersSources] .| joinLinesC
  where
  (headers, sources) = unzip headersSources
  header1 = case headers of
    [] -> Nothing
    (h1:_) -> h1
  -- let combine = if null optsSubrec envOpts then

preprocessHeader :: Functor f => Env ByteString -> f LineBS -> f LineBS
preprocessHeader env@Env{..} mheader = cut <$> mheader
  where
  cut = getSubrec env (optsSubrec envOpts)

preprocessC :: Monad m => Env ByteString -> p
  -> ConduitT i (LineString ByteString) m r
  -> ConduitT i (LineString ByteString) m r
preprocessC env@Env{..} _mheader source = mapOutput cut source
  where
  cut = getSubrec env (optsSubrec envOpts)
