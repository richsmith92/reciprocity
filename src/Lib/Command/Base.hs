module Lib.Command.Base (module Lib.Command.Base, module Options.Applicative, module Lib.Conduit) where

import CustomPrelude
import Lib.Base
import Lib.Conduit

import Options.Applicative hiding ((<>))

data CmdInfo c = CmdInfo {
  cmdDesc   :: Text,
  cmdParser :: Parser c
  }

class IsCommand c where
  runCommand :: Opts -> c -> IO ()
  commandInfo :: CmdInfo c

data Command = forall a. (Show a, IsCommand a) => Command a
deriving instance Show Command

-- * Option parsing

type OptParser a = Mod OptionFields a -> Parser a
textOpt :: Textual s => (s -> a) -> OptParser a
textOpt parse = option (parse . pack <$> str)

natOpt :: OptParser Natural
natOpt mods = option auto (mods ++ metavar "N")

natRgOpt :: OptParser (Pair Natural)
natRgOpt mods = textOpt (parse . map read . splitSeq ("," :: Text)) (mods ++ metavar "FROM[,TO]")
  where
  parse = \case
    [i] -> (i, i)
    [i, j] -> if i <= j then (i, j) else error "natRgOpt: FROM > TO"
    _ -> error "natRgOpt: unrecognized format"

keyOpt :: Parser SubRec
keyOpt = many (natRgOpt (long "key" ++ short 'k' ++ help "Key subrecord"))

valueOpt :: Parser SubRec
valueOpt = many (natRgOpt (long "val" ++ help "Value subrecord"))

funOpt :: Mod OptionFields Text -> Parser Text
funOpt mods = textOpt id (mods ++ metavar "FUN" ++ value "")
