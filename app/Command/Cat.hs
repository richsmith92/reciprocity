module Command.Cat where

import           CustomPrelude
import           Command.Common
import           System.FilePath       (addExtension, splitExtension)
import Data.NonNull
import qualified Language.Haskell.Interpreter as Hint
import           System.Directory      (getHomeDirectory)

data CmdCat = CmdCat {
  catExpr :: Text
  } deriving (Show)
instance IsCommand CmdCat where
  runCommand opts@Opts{..} cmd@CmdCat{..} = runConduitRes $
    catInputSources opts .| decodeUtf8C .| exprC opts cmd .| encodeUtf8C .| stdoutSink
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
