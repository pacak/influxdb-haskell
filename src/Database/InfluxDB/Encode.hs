{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Database.InfluxDB.Encode
  ( ToSeries(..)
  , ToSeriesData(..), toSeriesData
  , ToValue(..)
  ) where
import Data.Int (Int8, Int16, Int32, Int64)
import Data.Word (Word8, Word16, Word32)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL

import Database.InfluxDB.Types

-- | A type that can be converted to a 'Series'.
class ToSeries a where
  toSeries :: a -> Series a

-- | A type that can be stored in InfluxDB.
class ToValue a where
  toValue :: a -> Value

instance ToValue Value where
  toValue = id

instance ToValue Bool where
  toValue = Bool

instance ToValue a => ToValue (Maybe a) where
  toValue Nothing = Null
  toValue (Just a) = toValue a

instance ToValue Int where
  toValue = Int . fromIntegral

instance ToValue Int8 where
  toValue = Int . fromIntegral

instance ToValue Int16 where
  toValue = Int . fromIntegral

instance ToValue Int32 where
  toValue = Int . fromIntegral

instance ToValue Int64 where
  toValue = Int

instance ToValue Word8 where
  toValue = Int . fromIntegral

instance ToValue Word16 where
  toValue = Int . fromIntegral

instance ToValue Word32 where
  toValue = Int . fromIntegral

instance ToValue Double where
  toValue = Float

instance ToValue T.Text where
  toValue = String

instance ToValue TL.Text where
  toValue = String . TL.toStrict

instance ToValue String where
  toValue = String . T.pack
