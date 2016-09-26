module Main where

import           ClassyPrelude.Conduit hiding ((<.>))
import           Options.Applicative
import Language.Haskell.Interpreter
import           Data.ByteString          (appendFile, elemIndex)
import           Data.ByteString.Internal (c2w, w2c)
import           Data.Text                (replace)
import Data.Conduit.Zlib (gzip)
import System.IO (IOMode(..), withBinaryFile)
-- import Data.IOData as IO
import System.FilePath (splitExtension, addExtension)

data Command = CmdSplit Key [FilePath]
  deriving (Show)

data Opts = Opts {
  optsSep :: Text,
  optsHeader :: Bool
} deriving (Show)

type Fun = Text

data SubRec = Field Int | FullRec
  deriving (Show)

data Key = Key {
  keySubRec :: SubRec,
  keyFun :: Fun
} deriving Show

main :: IO ()
main = do
  (opts, cmd) <- execParser parser
  print (opts, cmd)
  runCmd opts cmd
  where
  parser = info (helper <*> ((,) <$> optsParser <*> cmdParser)) $ progDesc ""

optsParser :: Parser Opts
optsParser = do
  optsSep <- textOpt id (short 'd' ++ help "Delimiter" ++ value "\t")
  optsHeader <- switch (short 'H' ++ help "There is a header row")
  return Opts{..}

subrec i = if i == 0 then FullRec else Field (pred i)

keyOpt :: Parser Key
keyOpt = do
  sub <- option (subrec <$> auto) (short 'k' ++ value (Field 0))
  fun <- textOpt id (short 'f' ++ value "")
  return (Key sub fun)

cmdParser :: Parser Command
cmdParser = subparser $ mconcat
  [ cmd "split" "Split into multiple files" $ do
      key <- keyOpt
      files <- many $ strArgument (metavar "INPUT")
      return (CmdSplit key files)
  ]
  where
  cmd name desc parser = command name $ info (helper <*> parser) (progDesc desc)

textOpt :: Textual s => (s -> a) -> Mod OptionFields a -> Parser a
textOpt parse = option (parse . pack <$> str)

runCmd :: Opts -> Command -> IO ()
runCmd opts (CmdSplit key files) = do
  fk <- execKey opts key
  forM_ files $ \inFile -> runResourceT $
    sourceFile inFile $= linesUnboundedAsciiC $= decodeUtf8C $$
    sinkMultiHeaders (toFile inFile . fk)
    -- mapC (\s -> (fk $= unlinesC $$ stdoutC
  where
  -- source = if null files then stdinC else mapM_ sourceFile files
  toFile inFile key = let (file, ext) = splitExtension inFile in
    file `addExtension` unpack key `addExtension` ext

execKey :: (MonadIO m, MonadMask m) => Opts -> Key -> m (Text -> Text)
execKey opts Key{..} =  (. execSubRec opts keySubRec) <$> execFun keyFun

execSubRec :: Opts -> SubRec -> Text -> Text
execSubRec Opts{..} = \case
  FullRec -> id
  Field i -> (`indexEx` i) . splitSeq optsSep

execFun :: (MonadIO m, MonadMask m) => Text -> m (Text -> Text)
execFun "" = return id
execFun expr = either (error . show) id <.> runInterpreter $ do
  set [languageExtensions := [OverloadedStrings]]
  setImports ["ClassyPrelude"]
  interpret (unpack expr) id

lines' :: ByteString -> [ByteString]
lines' s = if null s then [] else
  maybe [s] (\n -> let (x, y) = splitAt (n + 1) s in x : lines' y) $ elemIndex (c2w '\n') s

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

whenC :: Bool -> (a -> a) -> a -> a
whenC b f = if b then f else id

-- | Point-free infix 'fmap'
infixl 4 <.>
(<.>) ::  Functor f => (b -> c) -> (a -> f b) -> a -> f c
f <.> g = fmap f . g
