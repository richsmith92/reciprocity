{-# LANGUAGE GADTs             #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Minimal set of utils we need most frequently.

module CustomPrelude (module CustomPrelude, module R) where

import ClassyPrelude.Conduit as R hiding ((<.>))
import Control.Arrow         as R ((<<<), (>>>))
import Control.Lens          as R hiding (Index, argument, cons, enum, index, snoc, uncons, unsnoc,
                                   (<&>), (<.>), (<|))

import qualified Control.Category         as Cat
import           Data.ByteString          (appendFile, elemIndex)
import           Data.ByteString.Internal as R (c2w, w2c)

import Data.Data     as R (Data)
import Data.Function as R (fix, (&))
import Data.List     as R (foldl1, foldr1, nub)
import Extra         as R (dupe, groupOn, groupSort, groupSortOn)

import Numeric.Natural as R

import System.Directory as R (createDirectoryIfMissing, doesFileExist, getHomeDirectory, removeFile)
import System.FilePath  as R (dropExtension, splitExtension, takeBaseName, takeDirectory,
                              takeExtension, takeFileName)

read :: (Read a, Textual s) => s -> a
read s = fromMaybe (error $ "read: " ++ unpack s) $ readMay s

type Pair a = (a, a)

showTsv, showTsvLn :: (IsString s, IsSequence s) => [s] -> s
showTsv = intercalate "\t"
showTsvLn = (++ "\n") . intercalate "\t"

-- readTsv :: (Textual s, IsSequence s) => s -> [s]
readTsv :: (IsString s, IsSequence s, Eq (Element s)) => s -> [s]
readTsv = splitSeq "\t"

readTsvLines :: (Textual s, Eq (Element s)) => s -> [[s]]
readTsvLines = map readTsv . lines

showTsvLines :: (IsString s, IsSequence s, Eq (Element s)) => [[s]] -> s
showTsvLines = concatMap showTsvLn
--
-- terr :: Text -> IO ()
-- terr = err . unpack
--
-- terrLn :: Text -> IO ()
-- terrLn = errLn . unpack

-- | Same as foldl', but with more convenient type
foldEndo' :: (MonoFoldable t) => (Element t -> b -> b) -> t -> b -> b
foldEndo' f = flip $ ofoldl' (flip f)

-- | Point-free infix 'fmap'
infixl 4 <.>
(<.>) ::  Functor f => (b -> c) -> (a -> f b) -> a -> f c
f <.> g = fmap f . g

enum :: (Enum a, Bounded a) => [a]
enum = enumFrom minBound

nan :: Floating a => a
nan = 0 / 0

nanToZero :: RealFloat a => a -> a
nanToZero x = if isNaN x then 0 else x

-- | Boolean operation for arrows.
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

toMaybe :: Bool -> a -> Maybe a
toMaybe b x = if b then Just x else Nothing

nonempty :: (MonoFoldable t) => a -> (t -> a) -> t -> a
nonempty def f xs = if null xs then def else f xs

-- | breakOnParts "-- ::" "yyyy-dd-mm hh:mm:ss" ==
--  ["yyyy", "dd", "mm", "hh", "mm", "ss"]
breakOnParts :: Eq a => [a] -> [a] -> [[a]]
breakOnParts [] xs = [xs]
breakOnParts _ [] = [[]]
breakOnParts (sep:seps) (x:xs) = if x == sep then [] : breakOnParts seps xs
  else let (part:parts) = breakOnParts (sep:seps) xs in (x : part) : parts

fromRight :: Either String a -> a
fromRight (Left s)  = error s
fromRight (Right x) = x
--
-- parallelMapM :: (a -> IO b) -> [a] -> IO [b]
-- parallelMapM f = parallel . map f
--
-- parallelMapM_ :: (a -> IO b) -> [a] -> IO ()
-- parallelMapM_ f = parallel_ . map f
--
-- parallelMap :: (a -> b) -> [a] -> [b]
-- parallelMap = parMap rpar


lines' :: ByteString -> [ByteString]
lines' s = if null s then [] else
  maybe [s] (\n -> let (x, y) = splitAt (n + 1) s in x : lines' y) $ elemIndex (c2w '\n') s
