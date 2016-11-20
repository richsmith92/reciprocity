{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Reciprocity.Internal where

import Prelude
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Internal as B
import           Data.Monoid ((<>))
import           Foreign.C.Types
-- import qualified Language.C.Inline as C
import qualified Data.Vector.Storable as V
import qualified Data.Vector.Storable.Mutable as MV
import           System.IO.Unsafe (unsafePerformIO)
import Data.Int
import Data.Word
import Foreign.Ptr
-- C.context (C.baseCtx <> C.bsCtx <> C.vecCtx)
-- C.include "<stdlib.h>"

foreign import ccall unsafe "static c_subrec" subrec_ffi
  :: CUChar -> Ptr CChar -> CLong -> CLong -> CLong -> Ptr CLong -> IO CInt

{-# INLINE c_subrec #-}
c_subrec :: Word8 -> (Int, Int) -> B.ByteString -> B.ByteString
c_subrec sep (start, end) = \s@(B.PS fPtr off _) -> unsafePerformIO $ {-# SCC v_do #-} do
    mv <- {-# SCC mv_new #-} MV.unsafeNew 2
    _ <- {-# SCC unsafeWith #-} MV.unsafeWith mv $ \mvPtr ->
      B.unsafeUseAsCStringLen s $ \(bsPtr, len) ->
      {-# SCC ffi #-} subrec_ffi (CUChar sep) bsPtr ({-# SCC _fromInt #-} fromIntegral len) start' end' mvPtr
    CLong subOff <- {-# SCC read #-} MV.read mv 0
    CLong subLen <- {-# SCC read #-} MV.read mv 1
    {-# SCC return_bs #-} return $ B.PS fPtr (off + fromIntegral subOff) (fromIntegral subLen)
  where
  (start', end') = (CLong $ fromIntegral start, CLong $ fromIntegral end)

--
-- {-# NOINLINE c_subrec' #-}
-- c_subrec' :: CUChar -> (CLong, CLong) -> B.ByteString -> IO (V.Vector CLong)
-- c_subrec' sep (start, end) bs = do
--   mv <- MV.unsafeNew 2
--   _ <- {-# SCC c #-} [C.block| int {
--       char sep = $(unsigned char sep), *s = $bs-ptr:bs;
--       long len = $bs-len:bs, end = $(long end), *mv = $vec-ptr:(long *mv);
--       long i = 0, k = 0;
--       while (k < $(long start) && i < len) {
--         if (s[i++] == sep) k++;
--       }
--       long i1 = i;
--       while (k <= $(long end) && i < len) {
--         if (s[i++] == sep) k++;
--       }
--       mv[0] = i1;
--       mv[1] = i == len && k <= end ? i - i1 : i - i1 - 1;
--       return 0;
--     } |]
--   V.freeze mv
