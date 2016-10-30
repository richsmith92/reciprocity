module Lib.Command.Builtin where

import CustomPrelude
import Lib.Base
import Lib.Command.Base
-- import Data.NonNull
import qualified Language.Haskell.Interpreter as Hint
import           System.Directory             (getHomeDirectory)

import Data.Conduit.Merge
import Data.Conduit.Zlib  (gzip)
import Data.List.Extra    (groupSort)
import Data.Text          (replace)
import System.IO          (IOMode (..), withBinaryFile)

-- * Cat

data CmdCat = CmdCat {
  catExpr :: Text
  } deriving (Show)
instance IsCommand CmdCat where
  runCommand opts@Opts{..} cmd@CmdCat{..} = runConduitRes $
    catInputSources opts .| linesC .| decodeUtf8C .| exprC opts cmd .| encodeUtf8C .| unlinesAsciiC .| stdoutC
  commandInfo = CmdInfo {
    cmdDesc = "Concatenate inputs and run given expression on them",
    cmdParser = do
      catExpr <- funOpt (short 'e' ++ long "expr" ++ help "Expression")
      return CmdCat{..}
    }

exprC :: (MonadIO m, Typeable m) => Opts -> CmdCat -> Conduit Text m Text
exprC Opts{..} CmdCat{..} = do
  when optsHeader $ takeC 1
  c <- if
    | null catExpr -> return $ awaitForever yield
    | otherwise -> withInterpreter $ Hint.interpret (unpack catExpr) (awaitForever yield)
  c

withInterpreter :: forall f b. MonadIO f => Hint.InterpreterT IO b -> f b
withInterpreter m = either (error . toMsg) id <.> liftIO $ Hint.runInterpreter $ do
  homeDir <- liftIO getHomeDirectory
  Hint.set [Hint.languageExtensions Hint.:= [Hint.OverloadedStrings]]
  Hint.loadModules [homeDir </> ".tsvtool/UserPrelude.hs"]
  Hint.setImportsQ [
    ("UserPrelude", Nothing),
    ("Prelude", Just "P")]
  m
  where
  toMsg = \case
    Hint.WontCompile errs -> unlines $ cons "***Interpreter***" $ map Hint.errMsg errs
    e -> show e

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

-- * Merge

data CmdMerge = CmdMerge deriving (Show)

instance IsCommand CmdMerge where
  commandInfo = CmdInfo {
    cmdDesc = "Merge ordered inputs into ordered output",
    cmdParser = pure CmdMerge
    }
  runCommand opts@Opts{..} _ = runConduitRes $ withInputSourcesH opts merge .| unlinesAsciiC .| stdoutC
    where
    merge = case optsKey of
      [] -> mergeSources
      _  -> mergeSourcesOn $ execKey opts

-- * Split

data CmdSplit = CmdSplit
  deriving (Show)
instance IsCommand CmdSplit where
  runCommand opts (CmdSplit) = sequence_ $
    zipWith (\file source -> runConduitRes $ source .| linesC .| splitSink opts file)
      (inputFiles opts) (inputSources opts)

  commandInfo = CmdInfo {
    cmdDesc = "Split into multiple files: put records having key KEY into file INPUT.KEY",
    cmdParser = pure CmdSplit
    }

-- * Partition

data CmdPartition = CmdPartition {
  partitionBuckets  :: Natural,
  partitionTemplate :: Text
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
