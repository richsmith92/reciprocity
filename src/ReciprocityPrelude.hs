{-# LANGUAGE GADTs             #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Minimal set of utils we need most frequently.

module ReciprocityPrelude (module ReciprocityPrelude, module R) where

import ClassyPrelude.Conduit as R hiding ((<.>))
import Control.Arrow         as R ((<<<), (>>>))
import Control.Lens          as R hiding
  (Index, argument, cons, enum, index, snoc, uncons, unsnoc, both, noneOf
  , (<&>), (<.>), (<|), (.=))
import Control.Error.Util          as R
  (bimapExceptT, err, errLn, exceptT, hoistEither, hush, note, (!?))

import qualified Control.Category         as Cat
import           Data.ByteString.Internal as R (c2w, w2c)

import Data.Data     as R (Data)
import Data.Function as R (fix, (&))
import Data.List     as R (foldl1, foldr1, nub)
import Extra         as R (dupe, groupOn, groupSort, groupSortOn)
import Data.Monoid as R hiding ((<>))

import Numeric.Natural as R
import System.Directory as R
  (createDirectoryIfMissing, doesFileExist, doesDirectoryExist
  , getHomeDirectory, removeFile, getHomeDirectory)
import System.FilePath  as R
  ( dropExtension, splitExtension, takeBaseName, takeDirectory, splitDirectories
  , takeExtension, takeFileName, joinPath, hasTrailingPathSeparator)

read :: (Read a, Textual s) => s -> a
read s = fromMaybe (error $ "read: " ++ unpack s) $ readMay s
{-# DEPRECATED read "You should use readMay or other safe functions" #-}

type Pair a = (a, a)

showTsv, showTsvLn :: (IsString s, IsSequence s) => [s] -> s
showTsv = intercalate "\t"
showTsvLn = (++ "\n") . intercalate "\t"

-- readTsv :: (Textual s, IsSequence s) => s -> [s]
readTsv :: (IsString s, IsSequence s, Eq (Element s)) => s -> [s]
readTsv = splitSeq "\t"

readTsvLines :: (Textual s, Eq (Element s)) => s -> [[s]]
readTsvLines = map readTsv . lines

showTsvLines :: (IsString s, IsSequence s) => [[s]] -> s
showTsvLines = concatMap showTsvLn

readTsvBs :: ByteString -> [ByteString]
readTsvBs = splitElem (c2w '\t')

terr :: Text -> IO ()
terr = err . unpack

terrLn :: Text -> IO ()
terrLn = errLn . unpack

-- | Wrapper for 'ofoldl\'' with more convenient type
foldEndo' :: (MonoFoldable t) => (Element t -> b -> b) -> t -> b -> b
foldEndo' f = flip $ ofoldl' (flip f)

-- | Point-free infix 'fmap'
infixl 4 <.>
(<.>) ::  Functor f => (b -> c) -> (a -> f b) -> a -> f c
f <.> g = fmap f . g

-- | Flipped infix 'fmap'
infixl 1 <&>
(<&>) :: Functor f => f a -> (a -> b) -> f b
as <&> f = f <$> as

-- @ enum = enumFrom minBound @
enum :: (Enum a, Bounded a) => [a]
enum = enumFrom minBound

-- prop> @nan = 0/0@
nan :: Floating a => a
nan = 0 / 0

nanToZero :: RealFloat a => a -> a
nanToZero x = if isNaN x then 0 else x

-- | Boolean operation for arrows.
-- > whenC True f = f
-- > whenC False _ = id
--
whenC :: (Cat.Category cat) => Bool -> cat a a -> cat a a
whenC b f = if b then f else Cat.id

-- | Opposite of 'whenC'
--
unlessC :: (Cat.Category cat) => Bool -> cat a a -> cat a a
unlessC b f = if b then Cat.id else f

-- | Boolean operation for monoids.
--
-- Returns its first argument when applied to `True',
-- returns `mempty' when applied to `False'.
mwhen :: (Monoid a) => Bool -> a -> a
mwhen b x = if b then x else mempty

munless :: (Monoid a) => Bool -> a -> a
munless b x = if b then mempty else x

toAlt :: Alternative f => Bool -> a -> f a
toAlt b a = if b then pure a else empty

nonempty :: (MonoFoldable t) => a -> (t -> a) -> t -> a
nonempty def f xs = if null xs then def else f xs

fromRight :: Either String a -> a
fromRight (Left s)  = error s
fromRight (Right x) = x
