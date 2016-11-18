module Reciprocity.Base where

import CustomPrelude
import qualified Data.ByteString as BS

data Opts = Opts {
  optsSep        :: Text,
  optsHeader     :: Bool,
  optsInputs     :: [FilePath]
  } deriving (Show)

data Env s = Env {
  envOpts :: Opts,
  envSplit :: s -> [s],
  envIntercalate :: [s] -> s,
  envSep :: s
  }

-- type StringLike a = (IsString a, IOData a, IsSequence a, Eq (Element a), Typeable a)
type StringLike s = (s ~ ByteString)

type Subrec = [Pair (Maybe Natural)]

getEnv :: Opts -> Env ByteString
getEnv opts@Opts{..} = Env {
  envOpts = opts,
  envSplit = if
    | Just (c, "") <- uncons sep -> splitElemEx c
    | otherwise -> splitSeq sep,
  envIntercalate = intercalate sep,
  envSep = sep
  }
  where
  sep = fromString (unpack optsSep)

{-# INLINE getSubrec #-}
getSubrec :: (StringLike s) => Env s -> Subrec -> s -> s
getSubrec env sub = runIdentity . (subrec env) sub Identity

{-# INLINE subrec #-}
subrec :: (StringLike s) => Env s -> Subrec -> Lens' s s
subrec Env{..} = \case
  [] -> id
  [rg] -> \f -> f . subrec (over both (fmap fromIntegral) rg)
  _ -> error "subrec: multiple ranges not implemented"
  where
  subrec r = if
    | (Just i, Just j) <- r -> envIntercalate . take (j - i + 1) . drop i . envSplit
    | (Just i, Nothing) <- r -> envIntercalate . drop i . envSplit
    | (Nothing, Just i) <- r -> envIntercalate . take (i + 1) . envSplit
    | (Nothing, Nothing) <- r -> error "subrec: no fields"

-- keyValue :: (StringLike s) => Env s -> Subrec -> Lens' s (s, s)

splitElemEx :: (IsSequence seq, Eq (Element seq)) => Element seq -> seq -> [seq]
splitElemEx x = splitWhen (== x)
{-# INLINE [0] splitElemEx #-}
{-# RULES "strict ByteString splitElemEx" splitElemEx = BS.split #-}
