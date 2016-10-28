module Command.Partition where

import Command.Common

import           CustomPrelude
import           Data.ByteString          (appendFile, elemIndex)
import           Data.ByteString.Internal (c2w, w2c)
import           Data.Text                (replace)
import Data.Conduit.Zlib (gzip)
import System.IO (IOMode(..), withBinaryFile)
import Data.List.Extra (groupSort)

data CmdPartition = CmdPartition {
  partitionBuckets    :: Natural,
  partitionTemplate   :: Text
  } deriving Show
instance IsCommand CmdPartition where
  runCommand opts CmdPartition{..} = do
    recs <- runConduitRes $ catInputSources opts .| sinkList
    let buckets = byBucket (execKey opts) partitionBuckets recs
    mapM_ (appendBucket opts partitionTemplate) buckets
  commandInfo = CmdInfo {
    cmdDesc = "Partition for MapReduce: split headerless input into N buckets determined by hash of the key",
    cmdParser = partitionParser
    }

partitionParser :: Parser CmdPartition
partitionParser = do
  partitionBuckets <- natOpt
    (short 'B' ++ long "buckets" ++ help "Number of buckets" ++ value 1)
  partitionTemplate <- argument (pack <$> str)
    (metavar "OUT_TEMPLATE" ++ help "Output filepath template")
  return CmdPartition{..}

appendBucket :: Opts -> Text -> (Int, [ByteString]) -> IO ()
appendBucket Opts{..} template (bucket, rows) = withBinaryFile file AppendMode $
  \h -> runResourceT $ yieldMany rows $= gzip $$ sinkHandle h
  where
  file = unpack $ replace optsReplaceStr (tshow bucket) template ++ ".gz"

byBucket :: (ByteString -> ByteString) -> Natural -> [ByteString] -> [(Int, [ByteString])]
byBucket fk n recs = groupSort [(1 + hash (fk r) `mod` fromIntegral n, r) | r <- recs]
