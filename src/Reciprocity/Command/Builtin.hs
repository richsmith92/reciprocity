module Reciprocity.Command.Builtin where

import CustomPrelude
import Reciprocity.Base
import Reciprocity.Command.Base
-- import Reciprocity.Hint
-- import Reciprocity.Compile
import Data.Char (isSpace)

-- import           System.Directory             (getHomeDirectory)
import Data.Text          (replace)
-- import Data.Conduit.Zlib  (gzip)
-- import Data.List.Extra    (groupSort)
-- import System.IO          (IOMode (..), withBinaryFile)
-- import System.Process

-- * Cat

data CmdCat = CmdCat deriving (Show)
instance IsCommand CmdCat where

  commandInfo = CmdInfo {
    cmdDesc = "Concatenate inputs",
    cmdParser = pure CmdCat
  }

  runCommand _ = do
    env <- ask
    liftIO $ runConduitRes $ withInputSourcesH env $
      \header sources -> (yieldMany header >> sequence_ sources) .| stdoutC

-- * Merge

data CmdMerge = CmdMerge { mergeKey :: Subrec } deriving (Show)

instance IsCommand CmdMerge where
  commandInfo = CmdInfo {
    cmdDesc = "Merge ordered inputs into ordered output",
    cmdParser = do
      mergeKey <- keyOpt
      pure CmdMerge{..}
    }
  runCommand CmdMerge{..} = do
    env <- ask
    let merge = mergeSourcesOn $ getSubrec env mergeKey
    runConduitRes $ withInputSourcesH env $
      \header sources -> (yieldMany header >> merge (map (.| linesC) sources) .| unlinesAsciiC) .| stdoutC
    where

-- * Join

data CmdJoin = CmdJoin {
  cmdJoinKey :: Subrec,
  cmdJoinValue :: Subrec,
  cmdJoinOuterLeft :: Bool,
  cmdJoinOuterRight :: Bool
  } deriving (Show)

instance IsCommand CmdJoin where

  commandInfo = CmdInfo {
    cmdDesc = "Join ordered headerless inputs on common key",
    cmdParser = do
      cmdJoinKey <- keyOpt
      cmdJoinValue <- valueOpt
      cmdJoinOuterLeft <- switch (short '1' ++ help "Outer join on first input")
      cmdJoinOuterRight <- switch (short '2' ++ help "Outer join on second input")
      return (CmdJoin{..})
    }

  runCommand CmdJoin{..} = do
    env <- ask
    let [s1, s2] = inputSources env
    runConduitRes $ joinCE (joinOpts env) (map (.| linesCE) [s1, s2]) .| unlinesCE .| stdoutC
    where
    {-# INLINE joinOpts #-}
    joinOpts env@Env{..} = JoinOpts {
      joinOuterLeft = cmdJoinOuterLeft,
      joinOuterRight = cmdJoinOuterRight,
      joinKey = getSubrec env cmdJoinKey,
      joinValue = getSubrec env cmdJoinValue,
      -- joinKeyValue = getKeyValue env cmdJoinKey cmdJoinValue,
      joinCombine = case cmdJoinKey of
        [] -> headEx
        _  -> \[k,v1,v2] -> k ++ envSep ++ v1 ++ envSep ++ v2
      }

-- * Split

data CmdSplit = CmdSplit {
  splitKey :: Subrec,
  splitPartition :: Bool,
  splitBuckets  :: Natural,
  splitTemplate :: Text
  } deriving (Show)
instance IsCommand CmdSplit where

  commandInfo = CmdInfo {
    cmdDesc = "Split into multiple files: put records having key KEY into file INPUT.KEY",
    cmdParser = do
      splitKey <- keyOpt
      splitPartition <- switch (long "partition" ++
        help "MapReduce partition mode: split into buckets determined by hash of the key")
      splitBuckets <- natOpt
        (long "buckets" ++ help "Number of buckets" ++ value 1)
      splitTemplate <- textOpt id (long "out" ++ value "{s}.{filename}" ++ help "Output filepath template (appended to input filename)")
      return CmdSplit{..}
    }

  runCommand (CmdSplit{..}) = do
    env <- ask
    let getKey = getSubrec env splitKey
    let split file source = runConduitRes $ withHeader env source $ \h -> linesCE .| splitCE (toFile getKey file) h
    sequence_ $ zipWith split (inputFiles env) (inputSources env)
    where
    toFile fk inFile = \rec -> unpack $ replace "{s}" (value rec) template
      where
      template = replace "{filename}" (pack $ takeFileName inFile) splitTemplate
      value = if
        | splitPartition -> tshow . bucket (fromIntegral splitBuckets) . fk
        | otherwise -> omap (\c -> if isSpace c then '.' else c) . decodeUtf8 . fk
    -- let (base, ext) = splitExtension inFile in base +? BC.unpack (fk rec) +? ext
    -- x +? y = if null x then y else if null y then x else x ++ "." ++ y

-- * Replace

data CmdReplace = CmdReplace {
  replaceDictFile :: FilePath,
  replaceSubrec, replaceDictKey, replaceDictValue :: Subrec
  } deriving Show
instance IsCommand CmdReplace where
  commandInfo = CmdInfo {
    cmdDesc = "Replace subrecord using dictionary",
    cmdParser = do
      replaceDictFile <- fileOpt $ long "dict" ++ help "Dictionary file"
      replaceDictKey <- keyOpt
      replaceDictValue <- valueOpt
      replaceSubrec <- subrecOpt $ long "sub" ++ help "Subrecord to replace"
      return CmdReplace{..}
    }

  runCommand (CmdReplace{..}) = do
    env <- ask
    let sub = subrec env replaceSubrec
    dict <- runConduitRes $ sourceFile replaceDictFile .| linesCE .| foldlCE
      (\m s -> uncurry insertMap (getKeyValue env replaceDictKey replaceDictValue s) m)
      (mempty :: HashMap ByteString _)
    let replaceC =  (.| linesCE .| dictReplaceCE dict sub .| unlinesCE)
    runConduitRes $ withInputSourcesH env $
      \header sources -> (yieldMany header >> mapM_ replaceC sources) .| stdoutC
