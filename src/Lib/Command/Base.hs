module Lib.Command.Base (module Lib.Command.Base, module Options.Applicative, module Lib.Conduit) where

import CustomPrelude
import Lib.Base
import Lib.Conduit

import Options.Applicative hiding ((<>))
import           System.Directory             (getHomeDirectory)

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

subrecOpt :: OptParser (Pair (Maybe Natural))
subrecOpt mods = textOpt (parse . splitSeq "-") (mods ++ metavar "FIELD|FROM-|-TO|FROM-TO")
  where
  parse :: [String] -> Pair (Maybe Natural)
  parse = \case
    [i] -> dupe $ field i
    ["", i] -> (Nothing, field i)
    [i, ""] -> (field i, Nothing)
    [i, j] -> (field i, field j)
    _ -> error "subrecOpt: unrecognized format"
  field = Just . pred . read

keyOpt :: Parser SubRec
keyOpt = many (subrecOpt (long "key" ++ short 'k' ++ help "Key subrecord"))

valueOpt :: Parser SubRec
valueOpt = many (subrecOpt (long "val" ++ help "Value subrecord"))

funOpt :: Mod OptionFields Text -> Parser Text
funOpt mods = textOpt id (mods ++ metavar "FUN" ++ value "")

-- * directory stuff

getRootDir :: IO FilePath
getRootDir = (</> ".tsvtool") <$> getHomeDirectory
