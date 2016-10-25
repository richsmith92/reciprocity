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

data SubRec = Field Natural | FullRec
  deriving (Show)

data Key = Key {
  keySubRec :: SubRec,
  keyFun :: Text
} deriving Show

type StringLike a = (IsString a, IOData a, IsSequence a, Eq (Element a), Typeable a)

keyOpt :: Parser Key
keyOpt = do
  sub <- subrec <$> natOpt (short 'k' ++ value 1 ++
    help "Key index (0 for whole record, default 1)")
  fun <- textOpt id (short 'f' ++ value "")
  return (Key sub fun)
  where
  subrec i = if i == 0 then FullRec else Field (pred i)

funOpt :: Mod OptionFields Text -> Parser Text
funOpt mods = textOpt id (mods ++ metavar "FUN" ++ value "")

execKey :: (MonadIO m, MonadMask m, StringLike a) => Opts -> Key -> m (a -> a)
execKey opts Key{..} =  (. execSubRec opts keySubRec) <$> execFun keyFun id

execSubRec :: (StringLike a) => Opts -> SubRec -> a -> a
execSubRec Opts{..} = \case
  FullRec -> id
  Field i -> (`indexEx` fromIntegral i) . splitSeq (fromString $ unpack optsSep)

execFun :: (MonadIO m, MonadMask m, Typeable a) => Text -> a -> m (a)
execFun "" x = return x
execFun expr x = either (error . toMsg) id <.> Hint.runInterpreter $ do
  Hint.set [Hint.languageExtensions Hint.:= [Hint.OverloadedStrings]]
  Hint.setImportsQ [
    ("ClassyPrelude.Conduit", Nothing),
    ("Prelude", Just "P"),
    ("Data.ByteString.Char8", Just "BC")]
  Hint.interpret (unpack expr) x
  where
  toMsg = \case
    Hint.WontCompile errs -> unlines $ cons "***Interpreter***" $ map Hint.errMsg errs
    e -> show e

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

textOpt :: Textual s => (s -> a) -> Mod OptionFields a -> Parser a
textOpt parse = option (parse . pack <$> str)

natOpt :: Mod OptionFields Natural -> Parser Natural
natOpt mods = option auto (mods ++ metavar "N")
