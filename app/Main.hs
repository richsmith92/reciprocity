module Main where

import CustomPrelude
import Lib.Base
import Lib.Command.Base
import Lib.Command.Builtin

main :: IO ()
main = do
  (Command cmd, opts) <- execParser parser
  -- print (opts, cmd)
  runCommand opts cmd
  where
  parser = info (helper <*> optsParser) $ progDesc ""

optsParser :: Parser (Command, Opts)
optsParser = do
  optsSep <- textOpt id (short 'd' ++ help "Delimiter (default is TAB)" ++ value "\t")
  optsHeader <- switch (short 'H' ++ help "Assume header row in each input")
  optsReplaceStr <- option (pack <$> str) (long "replacement-string" ++ value "{s}")
  optsKey <- keyOpt
  cmd <- commandParser
  optsInputs <- many (strArgument (metavar "INPUT"))
  return (cmd, Opts{..})

commandParser :: Parser Command
commandParser = subparser (mconcat
  [ sub "split" (commandInfo :: CmdInfo CmdSplit)
  , sub "merge"  (commandInfo :: CmdInfo CmdMerge)
  , sub "join" (commandInfo :: CmdInfo CmdJoin)
  , sub "run" (commandInfo :: CmdInfo CmdRun)
  ])
  where
  sub name (CmdInfo desc parser) = command name $ info (helper <*> (Command <$> parser)) (progDesc $ unpack desc)
