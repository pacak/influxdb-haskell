{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module Database.InfluxDB.Decode
  ( FromSeries(..), fromSeries
  , FromRow(..), fromSeriesData
  , withValues, (.:), (.:?), (.!=)
  , FromValue(..), fromValue
  , Parser, ValueParser, typeMismatch
  ) where

import Database.InfluxDB.Types

-- | A type that can be converted from a 'Series'.
class FromSeries a where
  parseSeries :: Series a -> Parser a

instance FromSeries SeriesData where
  parseSeries = return . seriesData

-- | Converte a value from a 'Series', failing if the types do not match.
fromSeries :: FromSeries a => Series a -> Either String a
fromSeries = runParser . parseSeries
