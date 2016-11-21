{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE BangPatterns #-}

module Reciprocity.Internal where

import qualified Data.ByteString as B
-- import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Internal as B
import           Foreign.C.Types
-- import Foreign.Ptr
import Foreign.ForeignPtr
import Foreign.Storable
import Foreign.Marshal.Array
import           System.IO.Unsafe (unsafePerformIO)
import Data.Int
import Data.Word
import Foreign.Ptr
import CustomPrelude

foreign import ccall unsafe "static c_subrec" subrec_ffi
  :: CUChar -> Ptr CChar -> CLong -> CLong -> CLong -> CLong -> Ptr CLong -> IO CInt

{-# INLINE getSub #-}
getSub :: Word8 -> (Int, Int) -> B.ByteString -> B.ByteString
getSub sep (start, end) = {-# SCC c_subrec #-} \(B.PS fPtr off len) -> unsafePerformIO $
  allocaArray 2 $ \aPtr -> {-# SCC allocated_array #-} do
    _ <- {-# SCC c_call #-} withForeignPtr fPtr $ \bsPtr ->
      subrec_ffi (CUChar sep) (castPtr bsPtr) (fromIntegral off) (fromIntegral len) start' end' aPtr
    x <- {-# SCC peek #-} peekElemOff aPtr 0
    y <- {-# SCC peek #-} peekElemOff aPtr 1
    {-# SCC return_bs #-} return $ B.PS fPtr (fromIntegral x) (fromIntegral y)
  where
  (start', end') = (CLong $ fromIntegral start, CLong $ fromIntegral end)

replaceSub :: ByteString -> ByteString -> ByteString -> ByteString
replaceSub (B.PS _ subOff subLen) newSub str@(B.PS _ off _) =
  B.take (subOff - off) str ++ newSub ++ B.drop (subOff - off + subLen) str

subLens :: Word8 -> (Int, Int) -> Lens' B.ByteString B.ByteString
subLens sep bounds = \f str -> let sub = getSub sep bounds str in
  (\sub' -> replaceSub sub sub' str) <$> f sub
