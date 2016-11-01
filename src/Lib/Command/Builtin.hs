module Lib.Command.Builtin where

import CustomPrelude
import Lib.Base
import Lib.Command.Base
-- import Lib.Hint
-- import Lib.Compile

-- import           System.Directory             (getHomeDirectory)
import Data.Text          (replace)
-- import Data.Conduit.Zlib  (gzip)
-- import Data.List.Extra    (groupSort)
-- import System.IO          (IOMode (..), withBinaryFile)
-- import System.Process

-- * Cat

data CmdCat = CmdCat deriving (Show)
instance IsCommand CmdCat where

  commandInfo = CmdInfo {
    cmdDesc = "Concatenate inputs",
    cmdParser = pure CmdCat
  }

  runCommand opts@Opts{..} _ = runConduitRes $ withInputSourcesH opts $
    \header sources -> (yieldMany header >> sequence_ sources) .| stdoutC

-- * Merge

data CmdMerge = CmdMerge deriving (Show)

instance IsCommand CmdMerge where
  commandInfo = CmdInfo {
    cmdDesc = "Merge ordered inputs into ordered output",
    cmdParser = pure CmdMerge
    }
  runCommand opts@Opts{..} _ = runConduitRes $ withInputSourcesH opts $
    \header sources -> (yieldMany header >> merge (map (.| linesC) sources) .| unlinesAsciiC) .| stdoutC
    where
    merge = case optsKey of
      [] -> mergeSourcesOn id
      _  -> mergeSourcesOn $ execKey opts

-- * Join

data CmdJoin = CmdJoin {
  joinValue :: SubRec
  } deriving (Show)

instance IsCommand CmdJoin where
  runCommand opts@Opts{..} CmdJoin{..} = do
    let [s1, s2] = [s .| linesCE | s <- inputSources opts]
    runConduitRes $ joinSources (execKey opts) (execSubRec opts joinValue) combine [s1, s2] .|
      unlinesCE .| stdoutC
    where
    combine = case optsKey of
      [] -> headEx
      _  -> intercalate sep
    sep = fromString (unpack optsSep)

  commandInfo = CmdInfo {
    cmdDesc = "Join ordered headerless inputs on common key",
    cmdParser = do
      joinValue <- valueOpt
      return (CmdJoin{..})
    }

-- * Split

data CmdSplit = CmdSplit {
  splitPartition :: Bool,
  splitBuckets  :: Natural,
  splitTemplate :: Text
  } deriving (Show)
instance IsCommand CmdSplit where

  commandInfo = CmdInfo {
    cmdDesc = "Split into multiple files: put records having key KEY into file INPUT.KEY",
    cmdParser = do
      splitPartition <- switch (long "partition" ++
        help "Emulate map-reduce partition: split into buckets determined by hash of the key")
      splitBuckets <- natOpt
        (long "buckets" ++ help "Number of buckets" ++ value 1)
      splitTemplate <- textOpt id (value ("{s}" ++ ".{filename}") ++
        metavar "OUT_TEMPLATE" ++ help "Output filepath template (appended to input filename)")
      return CmdSplit{..}
    }

  runCommand opts (CmdSplit{..}) = sequence_ $
    zipWith (\file source -> runConduitRes $ source .| linesC .| splitSink opts (toFile file))
    (inputFiles opts) (inputSources opts)
    where
    fk = execKey opts
    toFile inFile = \rec -> unpack $ replace (optsReplaceStr opts) (value rec) template
      where
      template = replace "{filename}" (pack $ takeFileName inFile) splitTemplate
      value = if
        | splitPartition ->  tshow . bucket (fromIntegral splitBuckets) . fk
        | otherwise ->  decodeUtf8 . fk
    -- let (base, ext) = splitExtension inFile in base +? BC.unpack (fk rec) +? ext
    -- x +? y = if null x then y else if null y then x else x ++ "." ++ y

bucket :: Int -> ByteString -> Int
bucket n s = 1 + hash s `mod` n

-- * Hint

--
-- data CmdPartition = CmdPartition {
--   partitionBuckets  :: Natural,
--   partitionTemplate :: Text
--   } deriving Show
-- instance IsCommand CmdPartition where
--   runCommand opts CmdPartition{..} = do
--     recs <- runConduitRes $ catInputSources opts .| sinkList
--     let buckets = byBucket (execKey opts) partitionBuckets recs
--     mapM_ (appendBucket opts partitionTemplate) buckets
--   commandInfo = CmdInfo {
--     cmdDesc = "Partition for MapReduce: split headerless input into N buckets determined by hash of the key",
--     cmdParser = partitionParser
--     }
--
-- partitionParser :: Parser CmdPartition
-- partitionParser = do
--   partitionBuckets <- natOpt
--     (short 'B' ++ long "buckets" ++ help "Number of buckets" ++ value 1)
--   partitionTemplate <- argument (pack <$> str)
--     (metavar "OUT_TEMPLATE" ++ help "Output filepath template")
--   return CmdPartition{..}
--
-- appendBucket :: Opts -> Text -> (Int, [ByteString]) -> IO ()
-- appendBucket Opts{..} template (bucket, rows) = withBinaryFile file AppendMode $
--   \h -> runResourceT $ yieldMany rows $= gzip $$ sinkHandle h
--   where
--   file = unpack $ replace optsReplaceStr (tshow bucket) template ++ ".gz"
--
-- byBucket :: (ByteString -> ByteString) -> Natural -> [ByteString] -> [(Int, [ByteString])]
-- byBucket fk n recs = groupSort [(1 + hash (fk r) `mod` fromIntegral n, r) | r <- recs]
