module Command.Common (module Command.Common, module Options.Applicative) where

import           CustomPrelude
import           Options.Applicative
import qualified  Language.Haskell.Interpreter as Hint
import           Data.ByteString.Internal (c2w, w2c)
import           Data.Text                (replace)
import Data.Conduit.Zlib (gzip)
import System.IO (IOMode(..), withBinaryFile)
import System.FilePath (splitExtension, addExtension)
import Data.Conduit.Zlib (ungzip)

--
-- class (Show a) => ToRec a where
--   toRec :: a -> ByteString
--   toRec = encodeUtf8 . tshow
-- instance ToRecs ByteString where
--   toRecs = (:[])

class IsCommand c where
  runCommand :: Opts -> c -> IO ()

data Command = forall a. (Show a, IsCommand a) => Command a
deriving instance Show Command

data Opts = Opts {
  optsSep :: Text,
  optsHeader :: Bool,
  optsReplaceStr :: Text
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

execSubRec :: (StringLike a) => Opts -> SubRec -> a -> a
execSubRec Opts{..} = \case
  [] -> id
  [rg] -> intercalate sep . subrec (over both fromIntegral rg) . splitSeq sep
  where
  subrec (i, j) = take (j - i + 1) . drop (i - 1)
  sep = fromString (unpack optsSep)

sinkMultiFile :: (IOData a, MonadIO m) => Sink (FilePath, a) m ()
sinkMultiFile = mapM_C $ \(file, line) ->  liftIO $
  withBinaryFile file AppendMode (`hPutStrLn` line)

sinkMultiHeader :: (IOData a, MonadIO m) => (a -> FilePath) -> Sink a m ()
sinkMultiHeader toFile = await >>= maybe (return ())
  (\header -> foldMC (go header) (mempty :: Set FilePath) >> return ())
  where
  go header files line = liftIO $ uncurry (withBinaryFile file) $ if
    | file `member` files -> (AppendMode , \h -> hPutStrLn h line >> return files)
    | otherwise -> (WriteMode,
      \h -> hPutStrLn h header >> hPutStrLn h line >> return (insertSet file files))
    where
    file = toFile line

sourceMultiHeader :: (MonadResource m) => [FilePath] -> Source m ByteString
sourceMultiHeader = \case
  [] -> return ()
  (file:files) -> src file >> mapM_ (($= tailC) . src) files
  where
  src = ($= linesUnboundedAsciiC) . sourceFile
  tailC = await >>= maybe (return ()) (const $ awaitForever yield)

sourceFiles :: (MonadResource m) => [FilePath] -> Source m ByteString
sourceFiles = mapM_ sourceDecompress

sourceDecompress :: (MonadResource m) => FilePath -> Source m ByteString
sourceDecompress file = sourceFile file .| decompress .| linesUnboundedAsciiC
  where
  decompress = if
    | ".gz" `isSuffixOf` file -> ungzip
    | otherwise -> awaitForever yield

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
