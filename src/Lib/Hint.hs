module Lib.Hint where

import CustomPrelude
import Lib.Base
import Lib.Command.Base

import qualified Language.Haskell.Interpreter as Hint

exprC :: (MonadIO m, Typeable m, Typeable a, Typeable b) => Opts -> Text -> Conduit a m b
exprC Opts{..} expr = do
  c <- if
    -- | null expr -> return $ awaitForever yield
    | otherwise -> liftIO $ withInterpreter $ Hint.interpret (unpack expr) (error "exprC")
  c

withInterpreter :: Hint.InterpreterT IO b -> IO b
withInterpreter m = either (error . toMsg) id <.> Hint.runInterpreter $ do
  Hint.set [Hint.languageExtensions Hint.:= [Hint.OverloadedStrings]]
  Hint.loadModules . (:[]) =<< liftIO getUserPrelude
  Hint.setImportsQ [
    ("UserPrelude", Nothing),
    ("Prelude", Just "P")]
  m
  where
  toMsg = \case
    Hint.WontCompile errs -> unlines $ cons "***Interpreter***" $ map Hint.errMsg errs
    e -> show e
