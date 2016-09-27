module Command.Common (module Command.Common, module Options.Applicative) where

import           ClassyPrelude.Conduit hiding ((<.>))
import           Options.Applicative
import Language.Haskell.Interpreter
import           Data.ByteString          (appendFile, elemIndex)
import           Data.ByteString.Internal (c2w, w2c)
import           Data.Text                (replace)
import Data.Conduit.Zlib (gzip)
import System.IO (IOMode(..), withBinaryFile)
import System.FilePath (splitExtension, addExtension)

class IsCommand c where
  runCommand :: Opts -> c -> IO ()

data Command = forall a. (Show a, IsCommand a) => Command a
deriving instance Show Command

data Opts = Opts {
  optsSep :: Text,
  optsHeader :: Bool,
  optsReplaceStr :: Text
  } deriving (Show)

type Fun = Text

data SubRec = Field Int | FullRec
  deriving (Show)

data Key = Key {
  keySubRec :: SubRec,
  keyFun :: Fun
} deriving Show

keyOpt :: Parser Key
keyOpt = do
  sub <- subrec <$> intOpt (short 'k' ++ value 1 ++
    help "Key index (0 for whole record, default 1)")
  fun <- textOpt id (short 'f' ++ value "")
  return (Key sub fun)
  where
  subrec i = if i == 0 then FullRec else Field (pred i)

type StringLike a = (IsString a, IOData a, IsSequence a, Eq (Element a), Typeable a)

execKey :: (MonadIO m, MonadMask m, StringLike a) => Opts -> Key -> m (a -> a)
execKey opts Key{..} =  (. execSubRec opts keySubRec) <$> execFun keyFun

execSubRec :: (StringLike a) => Opts -> SubRec -> a -> a
execSubRec Opts{..} = \case
  FullRec -> id
  Field i -> (`indexEx` i) . splitSeq (fromString $ unpack optsSep)

execFun :: (MonadIO m, MonadMask m, StringLike a) => Text -> m (a -> a)
execFun "" = return id
execFun expr = either (error . show) id <.> runInterpreter $ do
  set [languageExtensions := [OverloadedStrings]]
  setImports ["ClassyPrelude"]
  interpret (unpack expr) id

sinkMultiFile :: (IOData a, MonadIO m) => Sink (FilePath, a) m ()
sinkMultiFile = mapM_C $ \(file, line) ->  liftIO $
  withBinaryFile file AppendMode (`hPutStrLn` line)

sinkMultiHeaders :: (IOData a, MonadIO m) => (a -> FilePath) -> Sink a m ()
sinkMultiHeaders toFile = await >>= maybe (return ())
  (\header -> foldMC (go header) (mempty :: Set FilePath) >> return ())
  where
  go header files line = liftIO $ uncurry (withBinaryFile file) $ if
    | file `member` files -> (AppendMode , \h -> hPutStrLn h line >> return files)
    | otherwise -> (WriteMode,
      \h -> hPutStrLn h header >> hPutStrLn h line >> return (insertSet file files))
    where
    file = toFile line

textOpt :: Textual s => (s -> a) -> Mod OptionFields a -> Parser a
textOpt parse = option (parse . pack <$> str)

intOpt :: Mod OptionFields Int -> Parser Int
intOpt mods = option auto (mods ++ metavar "N")

whenC :: Bool -> (a -> a) -> a -> a
whenC b f = if b then f else id

-- | Point-free infix 'fmap'
infixl 4 <.>
(<.>) ::  Functor f => (b -> c) -> (a -> f b) -> a -> f c
f <.> g = fmap f . g

lines' :: ByteString -> [ByteString]
lines' s = if null s then [] else
  maybe [s] (\n -> let (x, y) = splitAt (n + 1) s in x : lines' y) $ elemIndex (c2w '\n') s
