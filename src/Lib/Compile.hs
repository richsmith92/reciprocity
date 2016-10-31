module Lib.Compile where

-- import Lib.Base
import Lib.Command.Base
import CustomPrelude
import Data.Digest.Pure.MD5
import System.Process

compileExpr :: [Text] -> Text -> IO FilePath
compileExpr ghcOpts expr = do
  dir <- getCacheDir
  let progSourceFile = dir </> basename ++ ".hs"
  let progExecFile = dir </> basename
  unlessM (doesFileExist progExecFile) $ do
    writeFile progSourceFile progSourceText
    compileFileWithArgs ghcOpts progSourceFile
  return progExecFile
  where
  basename = show $ md5 progSourceText
  progSourceText = sourceCode expr

sourceCode :: Text -> LByteString
sourceCode expr = intercalate "\n" [
    "{-# LANGUAGE ExtendedDefaultRules, NoImplicitPrelude, OverloadedStrings #-}",
    "import Lib.Conduit", "import CustomPrelude", "import qualified Data.ByteString.Char8 as BC",
    "main = runConduitRes (" ++ encodeUtf8 (fromStrict expr) ++ ")"
    ]


compileFileWithArgs :: [Text] -> FilePath -> IO ()
compileFileWithArgs args file = do
    -- absFilePath <- lift $ canonicalizePath file
    let args' = ["ghc", "--", file , "-v0" ] ++ map unpack args
    out <- readProcess "stack" args' ""
    putStr $ pack out
