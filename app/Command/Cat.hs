module Command.Cat where

import           CustomPrelude
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)
import Data.NonNull
import qualified Language.Haskell.Interpreter as Hint
import           System.Directory      (getHomeDirectory)

data CmdCat = CmdCat {
  catInputs :: [FilePath],
  catExpr :: Text
  } deriving (Show)
instance IsCommand CmdCat where
  runCommand opts@Opts{..} cmd@CmdCat{..} = runConduitRes $
    source catInputs .| decodeUtf8C .|
    exprC opts cmd .|
    unlinesC .| encodeUtf8C .| stdoutC
    where
    source = if optsHeader then sourceMultiHeader else sourceFiles

exprC :: (MonadIO m, Typeable m) => Opts -> CmdCat -> Conduit Text m Text
exprC Opts{..} CmdCat{..} = do
  when optsHeader $ takeC 1
  c <- if
    | null catExpr -> return $ awaitForever yield
    | otherwise -> withInterpreter $ Hint.interpret (unpack catExpr) (awaitForever yield)
  c


-- withInterpreter :: (MonadIO m, MonadMask m, Typeable a) => Text -> a -> m (a)
-- withInterpreter "" x = return x
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

-- runAccumC :: MonadIO m => Opts -> CmdCat -> Conduit ByteString m ByteString
-- runAccumC Opts{..} CmdCat{..} = do
--   when optsHeader $ takeC 1
--   f <- liftIO $ withInterpreter catAccumFun (const id)
--   accumC f catWindow

accumC :: Monad m => (Seq a -> a -> b) -> Natural -> Conduit a m b
accumC f n = do
  go . reverse . fromList =<< (takeC (fromIntegral n) .| sinkList)
  where
  go w = await >>= maybe (return ()) (\x -> yield (f w x) >> go (init . ncons x $ w))

catParser :: Parser CmdCat
catParser = do
  catInputs <- many $ strArgument (metavar "INPUT")
  catExpr <- funOpt (short 'e' ++ long "expr" ++ help "Expression")
  return CmdCat{..}
