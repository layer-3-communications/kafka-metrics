{-# language
    BangPatterns
  , MagicHash
  , TypeApplications
  , ViewPatterns
  #-}

module Kafka.Metrics
  ( -- * Types
    Poll(..)
  , Report(..)

    -- * Production
  , success
  , failure
  , noMessage
  , decode

    -- * Logging
  , formatReport
  ) where

import Control.Monad.ST (runST)
import Data.ByteString (ByteString)
import Data.ByteString.Internal (ByteString(..))
import Data.Primitive.ByteArray
import GHC.Exts
import GHC.ForeignPtr
import Numeric (showFFloat)

import qualified Kafka.Consumer as Kafka

-- | The result of decoding a fetch response request
--   from Kafka ('Kafka.FetchResponse'). It includes
--   all successfully decoded items and a report
--   containing metrics about the results of decoding.
data Poll a = Poll [a] {-# UNPACK #-} !Report

-- | A report about the decoding.
data Report = Report
  { successes  :: {-# UNPACK #-} !Int
    -- ^ number of messages that successfully decoded.
  , failures   :: {-# UNPACK #-} !Int
    -- ^ number of messages that failed to decode.
  , noMessages :: {-# UNPACK #-} !Int
    -- ^ number of messages that were empty.
  }

instance Semigroup Report where
  Report a b c <> Report a' b' c'
    = Report (a + a') (b + b') (c + c')
instance Monoid Report where
  mempty = Report 0 0 0

instance Semigroup (Poll a) where
  Poll as r <> Poll as' r'
    = Poll (as <> as') (r <> r')
instance Monoid (Poll a) where
  mempty = Poll mempty mempty

success   :: a -> Poll a
failure   ::      Poll a
noMessage ::      Poll a

success a = Poll [a] (Report 1 0 0)
failure   = Poll [ ] (Report 0 1 0)
noMessage = Poll [ ] (Report 0 0 1)

decode :: (ByteString -> Poll a) -> Kafka.FetchResponse -> Poll a
decode f resp = mconcat $ do
  partitions <- do
    topic <- Kafka.topics resp
    Kafka.partitions topic
  case Kafka.recordSet partitions of
    Nothing -> do
      pure noMessage
    Just recordSet -> do
      record <- do
        batch <- recordSet
        Kafka.records batch
      case Kafka.recordValue record of
        Nothing -> do
          pure noMessage
        Just (byteArrayToByteString -> bytes) -> do
          pure (f bytes)

-- | Format (pretty print) a 'Report'. This is intended for logging.
--   'String' was chosen in order to avoid any one particular log
--   type.
formatReport :: Report -> String
formatReport (Report s f n) =
  let intToDouble = fromIntegral @Int @Double
      total = s + f + n
      succRate = showFFloat (Just 3) (100.0 * intToDouble s / intToDouble total) ""
    in mempty
      <> "Poll Statistics: "
      <> "success rate = " <> succRate <> ","
      <> "successes = " <> show s <> ","
      <> "failures = " <> show f <> ","
      <> "no_msg = " <> show n <> ","
      <> "total = " <> show total

unsafeByteArrayToByteString :: ByteArray -> ByteString
unsafeByteArrayToByteString (ByteArray b#) =
  let addr# = byteArrayContents# b#
      fp = ForeignPtr addr# (PlainPtr (unsafeCoerce# b#))
      len = I# (sizeofByteArray# b#)
  in PS fp 0 len

byteArrayToByteString :: ByteArray -> ByteString
byteArrayToByteString b@(ByteArray b#)
  | isTrue# (isByteArrayPinned# b#) = unsafeByteArrayToByteString b
  | otherwise = runST $ do
      let len = sizeofByteArray b
      marr@(MutableByteArray marr#) <- newPinnedByteArray len
      copyByteArray marr 0 b 0 len
      let addr# = byteArrayContents# (unsafeCoerce# marr#)
      let fp = ForeignPtr addr# (PlainPtr (unsafeCoerce# marr#))
      pure (PS fp 0 len)
