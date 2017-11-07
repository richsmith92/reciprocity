module Main where

import ReciprocityPrelude
import Reciprocity.Base
import Reciprocity.Command.Base
import Reciprocity.Command.Builtin

main :: IO ()
main = do
  (Command cmd, opts) <- execParser parser
  -- print (opts, cmd)
  runReaderT (runCommand cmd) (getEnv opts)
  where
  parser = info (helper <*> liftA2 (,) commandParser optsParser) $ progDesc ""
--
-- optsParser :: Parser (Command, Opts)
-- optsParser = do
--   optsSep <- textOpt id (short 'd' ++ help "Delimiter (default is TAB)" ++ value "\t")
--   optsHeader <- switch (short 'H' ++ help "Assume header row in each input")
--   cmd <- commandParser
--   optsInputs <- many (strArgument (metavar "INPUT"))
--   return (cmd, Opts{..})

commandParser :: Parser Command
commandParser = subparser (mconcat
  [ sub "split" (commandInfo :: CmdInfo CmdSplit)
  , sub "merge"  (commandInfo :: CmdInfo CmdMerge)
  , sub "join" (commandInfo :: CmdInfo CmdJoin)
  , sub "cat" (commandInfo :: CmdInfo CmdCat)
  , sub "replace" (commandInfo :: CmdInfo CmdReplace)
  , sub "diff" (commandInfo :: CmdInfo CmdDiff)
  ])
  where
  sub name (CmdInfo desc parser) = command name $ info (helper <*> (Command <$> parser)) (progDesc $ unpack desc)
