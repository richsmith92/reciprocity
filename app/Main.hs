module Main where

import Command.Common
import Command.Split
import Command.Partition
import Command.Cat
import Command.Merge

import           ClassyPrelude.Conduit hiding ((<.>))
import           Options.Applicative

main :: IO ()
main = do
  (opts, Command cmd) <- execParser parser
  -- print (opts, cmd)
  runCommand opts cmd
  where
  parser = info (helper <*> ((,) <$> optsParser <*> cmdParser)) $ progDesc ""

optsParser :: Parser Opts
optsParser = do
  optsSep <- textOpt id (short 'd' ++ help "Delimiter" ++ value "\t")
  optsHeader <- switch (short 'H' ++ help "There is a header row")
  optsReplaceStr <- option (pack <$> str) (long "replacement-string" ++ value "%s")
  return Opts{..}

cmdParser :: Parser Command
cmdParser = subparser $ mconcat
  [ cmd "split" "Split into multiple files" splitParser
  , cmd "partition" "Partition for MapReduce" partitionParser
  , cmd "cat" "Concatenate files, omitting extra headers" catParser
  , cmd "merge" "Merge sorted headerless files" mergeParser
  ]
  where
  cmd name desc parser = command name $ info (helper <*> (Command <$> parser)) (progDesc desc)
