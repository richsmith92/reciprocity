module Reciprocity.Command.Base (
  module Reciprocity.Command.Base,
  module Options.Applicative,
  module Reciprocity.Conduit) where

import ReciprocityPrelude
import Reciprocity.Base
import Reciprocity.Conduit

import Options.Applicative
import           System.Directory             (getHomeDirectory)

data CmdInfo c = CmdInfo {
  cmdDesc   :: Text,
  cmdParser :: Parser c
  }

class IsCommand c where
  runCommand :: c -> ReaderT (Env ByteString) IO ()
  commandInfo :: CmdInfo c

data Command = forall a. (Show a, IsCommand a) => Command a
deriving instance Show Command

-- * Option parsing

type OptParser a = Mod OptionFields a -> Parser a

optsParser :: Parser Opts
optsParser = do
  optsSep <- textOpt id (short 'd' ++ help "Delimiter (default is TAB)" ++ value "\t")
  optsHeader <- not <$> switch (short 'H' ++ long "headerless" ++ help "No header line in inputs")
  optsInputs <- many (strArgument (metavar "INPUT"))
  optsSubrec <- subrecOpt (long "subrec" ++ short 'c' ++ help "Take subrecord of each input")
  return (Opts{..})

fileOpt :: OptParser FilePath
fileOpt mods = option str (mods ++ metavar "FILE")

textOpt :: Textual s => (s -> a) -> OptParser a
textOpt parse = option (parse . pack <$> str)

natOpt :: OptParser Natural
natOpt mods = option auto (mods ++ metavar "N")

subrecOpt mods = many $ fieldRangeOpt mods

-- fieldRangeOpt :: OptParser Subrec
fieldRangeOpt :: OptParser FieldRange
fieldRangeOpt mods = textOpt (parse . splitSeq "-") (mods ++ metavar "SUBREC")
  where
  parse :: [String] -> FieldRange
  parse = \case
    [i] ->     (field i,field i)
    -- ["",""] -> []
    ["", i] -> (Nothing, field i)
    [i, ""] -> (field i, Nothing)
    [i, j] ->  (field i, field j)
    _ -> error "fieldRangeOpt: unrecognized format"
  field = Just . pred . read

keyOpt :: Parser Subrec
keyOpt = subrecOpt (long "key" ++ short 'k' ++ help "Key subrecord")

valueOpt :: Parser Subrec
valueOpt = subrecOpt (long "val" ++ short 'v' ++ help "Value subrecord")

funOpt :: Mod OptionFields Text -> Parser Text
funOpt mods = textOpt id (mods ++ metavar "FUN" ++ value "")

-- * directory stuff

getRootDir :: IO FilePath
getRootDir = (</> ".reciprocity") <$> getHomeDirectory
