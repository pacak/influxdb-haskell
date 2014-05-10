{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeSynonymInstances #-}

{-# LANGUAGE OverlappingInstances #-}
module Database.InfluxDB.Types
  ( -- * Series, columns and data points
    Series(..), seriesColumns, seriesPoints
  , SeriesData(..)
  , Column
  , Value(..)

  , ToSeriesData(..), toSeriesData

  , FromRow(..), fromSeriesData
  , withValues, (.:), (.:?), (.!=)

  , FromValue(..), fromValue

  , Parser, ValueParser, runParser, typeMismatch

  -- * Data types for HTTP API
  , Credentials(..)
  , Server(..)
  , Database(..)
  , ScheduledDelete(..)
  , ContinuousQuery(..)
  , User(..)
  , Admin(..)
  , Ping(..)
  , Interface

  -- * Server pool
  , ServerPool
  , serverRetrySettings
  , newServerPool
  , newServerPoolWithRetrySettings
  , activeServer
  , failover
  ) where

import Control.Applicative
import Control.Monad.Reader
import Data.Data (Data)
import Data.IORef
import Data.Int
import Data.Map (Map)
import Data.Maybe (fromMaybe)
import Data.Proxy
import Data.Sequence (Seq, ViewL(..), (|>))
import Data.Text (Text)
import Data.Tuple (swap)
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Data.Word
import qualified Data.Map as Map
import qualified Data.Sequence as Seq
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Vector as V

import Control.Retry (RetrySettings(..), limitedRetries)
import Data.Aeson.TH
import qualified Data.Aeson as A

import Database.InfluxDB.Types.Internal (stripPrefixOptions)

#if MIN_VERSION_aeson(0, 7, 0)
import Data.Scientific
#else
import Data.Attoparsec.Number
#endif

-----------------------------------------------------------
-- Compatibility for older GHC

#if __GLASGOW_HASKELL__ < 706
import Control.Exception (evaluate)

atomicModifyIORef' :: IORef a -> (a -> (a, b)) -> IO b
atomicModifyIORef' ref f = do
    b <- atomicModifyIORef ref $ \x ->
      let (a, b) = f x
      in (a, a `seq` b)
    evaluate b
#endif
-----------------------------------------------------------

-- | A series consists of name, columns and points. The columns and points are
-- expressed in a separate type 'SeriesData'.
data Series a = Series
  { seriesName :: {-# UNPACK #-} !Text
  -- ^ Series name
  , seriesData :: !a
  -- ^ Columns and data points in the series
  } deriving (Eq, Show)

-- | Convenient accessor for columns.
seriesColumns
  :: ToSeriesData a
  => Series a
  -> Vector Column
seriesColumns = seriesDataColumns . toSeriesData . seriesData

-- | Convenient accessor for points.
seriesPoints
  :: ToSeriesData a
  => Series a
  -> [Vector Value]
seriesPoints = seriesDataPoints . toSeriesData . seriesData

instance A.ToJSON (Series SeriesData) where
  toJSON Series {..} = A.object
    [ "name" A..= seriesName
    , "columns" A..= seriesDataColumns
    , "points" A..= seriesDataPoints
    ]
    where
      SeriesData {..} = seriesData

instance ToSeriesData a => A.ToJSON (Series a) where
  toJSON Series {..} = A.object
    [ "name" A..= seriesName
    , "columns" A..= seriesDataColumns
    , "points" A..= seriesDataPoints
    ]
    where
      SeriesData {..} = toSeriesData seriesData

instance A.FromJSON (Series SeriesData) where
  parseJSON (A.Object v) = do
    seriesName <- v A..: "name"
    columns <- v A..: "columns"
    points <- v A..: "points"
    let seriesData = SeriesData
          { seriesDataColumns = columns
          , seriesDataPoints = points
          }
    return Series
      { seriesName
      , seriesData
      }
  parseJSON _ = empty

instance FromRow a => A.FromJSON (Series [a]) where
  parseJSON (A.Object v) = do
    seriesName <- v A..: "name"
    columns <- v A..: "columns"
    points <- v A..: "points"
    let seriesData = SeriesData
          { seriesDataColumns = columns
          , seriesDataPoints = points
          }
    case fromSeriesData seriesData of
      Left reason -> fail reason
      Right xs -> return Series
        { seriesName
        , seriesData = xs
        }
  parseJSON _ = empty

-- | 'SeriesData' consists of columns and points.
data SeriesData = SeriesData
  { seriesDataColumns :: Vector Column
  , seriesDataPoints :: [Vector Value]
  } deriving (Eq, Show)

type Column = Text

-- | An InfluxDB value represented as a Haskell value.
data Value
  = Int !Int64
  | Float !Double
  | String !Text
  | Bool !Bool
  | Null
  deriving (Eq, Show, Data, Typeable)

instance A.ToJSON Value where
  toJSON (Int n) = A.toJSON n
  toJSON (Float d) = A.toJSON d
  toJSON (String xs) = A.toJSON xs
  toJSON (Bool b) = A.toJSON b
  toJSON Null = A.Null

instance A.FromJSON Value where
  parseJSON (A.Object o) = fail $ "Unexpected object: " ++ show o
  parseJSON (A.Array a) = fail $ "Unexpected array: " ++ show a
  parseJSON (A.String xs) = return $ String xs
  parseJSON (A.Bool b) = return $ Bool b
  parseJSON A.Null = return Null
  parseJSON (A.Number n) = return $! numberToValue
    where
#if MIN_VERSION_aeson(0, 7, 0)
      numberToValue
        | e < 0 = Float $ realToFrac n
        | otherwise = Int $ fromIntegral $ coefficient n * 10 ^ e
        where
          e = base10Exponent n
#else
      numberToValue = case n of
        I i -> Int $ fromIntegral i
        D d -> Float d
#endif

-----------------------------------------------------------

-- | A type that can be converted to a 'SeriesData'. A typical implementation is
-- as follows.
--
-- > import qualified Data.Vector as V
-- >
-- > data Event = Event Text EventType
-- > data EventType = Login | Logout
-- >
-- > instance ToSeriesData Event where
-- >   toSeriesColumn _ = V.fromList ["user", "type"]
-- >   toSeriesPoints (Event user ty) = V.fromList [toValue user, toValue ty]
-- >
-- > instance ToValue EventType
class ToSeriesData a where
  -- | Column names. You can safely ignore the proxy agument.
  toSeriesColumns :: Proxy a -> Vector Column
  -- | Data points in a record.
  toSeriesPoints :: a -> Vector Value

toSeriesData :: forall a. ToSeriesData a => a -> SeriesData
toSeriesData a = SeriesData
  { seriesDataColumns = toSeriesColumns (Proxy :: Proxy a)
  , seriesDataPoints = [toSeriesPoints a]
  }

-----------------------------------------------------------

-- | A type that can be converted from a 'SeriesData'. A typical implementation
-- is as follows.
--
-- > import Control.Applicative ((<$>), (<*>))
-- > import qualified Data.Vector as V
-- >
-- > data Event = Event Text EventType
-- > data EventType = Login | Logout
-- >
-- > instance FromSeriesData Event where
-- >   parseSeriesData = withValues $ \values -> Event
-- >     <$> values .: "user"
-- >     <*> values .: "type"
-- >
-- > instance FromValue EventType
parseSeriesData :: FromRow a => SeriesData -> Parser [a]
parseSeriesData SeriesData {..} =
  mapM (parseRow seriesDataColumns) seriesDataPoints

-- | Converte a value from a 'SeriesData', failing if the types do not match.
fromSeriesData :: FromRow a => SeriesData -> Either String [a]
fromSeriesData = runParser . parseSeriesData

class FromRow a where
  parseRow :: Vector Column -> Vector Value -> Parser a

fromRow :: FromRow a => Vector Column -> Vector Value -> Either String a
fromRow = (runParser .) . parseRow

-- | Helper function to define 'parseSeriesData' from 'ValueParser's.
withValues
  :: (Vector Value -> ValueParser a)
  -> Vector Column -> Vector Value -> Parser a
withValues f columns values =
  runReaderT m $ Map.fromList $ map swap $ V.toList $ V.indexed columns
  where
    ValueParser m = f values

-- | Retrieve the value associated with the given column. The result is 'empty'
-- if the column is not present or the value cannot be converted to the desired
-- type.
(.:) :: FromValue a => Vector Value -> Column -> ValueParser a
values .: column = do
  found <- asks $ Map.lookup column
  case found of
    Nothing -> fail $ "No such column: " ++ T.unpack column
    Just idx -> do
      value <- V.indexM values idx
      liftParser $ parseValue value

-- | Retrieve the value associated with the given column. The result is
-- 'Nothing' if the column is not present or the value cannot be converted to
-- the desired type.
(.:?) :: FromValue a => Vector Value -> Column -> ValueParser (Maybe a)
values .:? column = do
  found <- asks $ Map.lookup column
  case found of
    Nothing -> return Nothing
    Just idx ->
      case values V.!? idx of
        Nothing -> return Nothing
        Just value -> liftParser $ parseValue value

-- | Helper for use in combination with '.:?' to provide default values for
-- optional columns.
(.!=) :: Parser (Maybe a) -> a -> Parser a
p .!= def = fromMaybe def <$> p

newtype Parser a = Parser
  { runParser :: Either String a
  } deriving (Functor, Applicative, Monad)

type ColumnIndex = Map Column Int

newtype ValueParser a = ValueParser (ReaderT ColumnIndex Parser a)
  deriving (Functor, Applicative, Monad, MonadReader ColumnIndex)

liftParser :: Parser a -> ValueParser a
liftParser = ValueParser . ReaderT . const


-- | A type that can be converted from a 'Value'.
class FromValue a where
  parseValue :: Value -> Parser a

-- | Converte a value from a 'Value', failing if the types do not match.
fromValue :: FromValue a => Value -> Either String a
fromValue = runParser . parseValue

instance FromValue Value where
  parseValue = return

instance FromValue Bool where
  parseValue (Bool b) = return b
  parseValue v = typeMismatch "Bool" v

instance FromValue a => FromValue (Maybe a) where
  parseValue Null = return Nothing
  parseValue v = Just <$> parseValue v

instance FromValue Int where
  parseValue (Int n) = return $ fromIntegral n
  parseValue v = typeMismatch "Int" v

instance FromValue Int8 where
  parseValue (Int n)
    | n <= fromIntegral (maxBound :: Int8) = return $ fromIntegral n
    | otherwise = fail $ "Larger than the maximum Int8: " ++ show n
  parseValue v = typeMismatch "Int8" v

instance FromValue Int16 where
  parseValue (Int n)
    | n <= fromIntegral (maxBound :: Int16) = return $ fromIntegral n
    | otherwise = fail $ "Larger than the maximum Int16: " ++ show n
  parseValue v = typeMismatch "Int16" v

instance FromValue Int32 where
  parseValue (Int n)
    | n <= fromIntegral (maxBound :: Int32) = return $ fromIntegral n
    | otherwise = fail $ "Larger than the maximum Int32: " ++ show n
  parseValue v = typeMismatch "Int32" v

instance FromValue Int64 where
  parseValue (Int n)
    | n <= fromIntegral (maxBound :: Int64) = return $ fromIntegral n
    | otherwise = fail $ "Larger than the maximum Int64: " ++ show n
  parseValue v = typeMismatch "Int64" v

instance FromValue Word8 where
  parseValue (Int n)
    | n <= fromIntegral (maxBound :: Word8) = return $ fromIntegral n
    | otherwise = fail $ "Larger than the maximum Word8: " ++ show n
  parseValue v = typeMismatch "Word8" v

instance FromValue Word16 where
  parseValue (Int n)
    | n <= fromIntegral (maxBound :: Word16) = return $ fromIntegral n
    | otherwise = fail $ "Larger than the maximum Word16: " ++ show n
  parseValue v = typeMismatch "Word16" v

instance FromValue Word32 where
  parseValue (Int n)
    | n <= fromIntegral (maxBound :: Word32) = return $ fromIntegral n
    | otherwise = fail $ "Larger than the maximum Word32: " ++ show n
  parseValue v = typeMismatch "Word32" v

instance FromValue Double where
  parseValue (Float d) = return d
  parseValue v = typeMismatch "Float" v

instance FromValue T.Text where
  parseValue (String xs) = return xs
  parseValue v = typeMismatch "Text" v

instance FromValue TL.Text where
  parseValue (String xs) = return $ TL.fromStrict xs
  parseValue v = typeMismatch "lazy Text" v

instance FromValue String where
  parseValue (String xs) = return $ T.unpack xs
  parseValue v = typeMismatch "String" v

typeMismatch
  :: String
  -> Value
  -> Parser a
typeMismatch expected actual = fail $
  "when expecting a " ++ expected ++
  ", encountered " ++ name ++ " instead"
  where
    name = case actual of
      Int _ -> "Int"
      Float _ -> "Float"
      String _ -> "String"
      Bool _ -> "Bool"
      Null -> "Null"

-----------------------------------------------------------

-- | User credentials.
data Credentials = Credentials
  { credsUser :: !Text
  , credsPassword :: !Text
  } deriving Show

-- | Server location.
data Server = Server
  { serverHost :: !Text
  -- ^ Hostname or IP address
  , serverPort :: !Int
  , serverSsl :: !Bool
  -- ^ SSL is enabled or not in the server side
  } deriving Show

-- | Non-empty set of server locations. The active server will always be used
-- until any HTTP communications fail.
data ServerPool = ServerPool
  { serverActive :: !Server
  -- ^ Current active server
  , serverBackup :: !(Seq Server)
  -- ^ The rest of the servers in the pool.
  , serverRetrySettings :: !RetrySettings
  }

newtype Database = Database
  { databaseName :: Text
  } deriving Show

newtype ScheduledDelete = ScheduledDelete
  { scheduledDeleteId :: Int
  } deriving Show

data ContinuousQuery = ContinuousQuery
  { continuousQueryId :: !Int
  , continuousQueryQuery :: !Text
  } deriving Show

-- | User
data User = User
  { userName :: Text
  , userIsAdmin :: Bool
  } deriving Show

-- | Administrator
newtype Admin = Admin
  { adminName :: Text
  } deriving Show

newtype Ping = Ping
  { pingStatus :: Text
  } deriving Show

type Interface = Text

-----------------------------------------------------------
-- Server pool manipulation

-- | Create a non-empty server pool. You must specify at least one server
-- location to create a pool.
newServerPool :: Server -> [Server] -> IO (IORef ServerPool)
newServerPool = newServerPoolWithRetrySettings defaultRetrySettings
  where
    defaultRetrySettings = RetrySettings
      { numRetries = limitedRetries 5
      , backoff = True
      , baseDelay = 50
      }

newServerPoolWithRetrySettings
    :: RetrySettings -> Server -> [Server] -> IO (IORef ServerPool)
newServerPoolWithRetrySettings retrySettings active backups =
  newIORef ServerPool
    { serverActive = active
    , serverBackup = Seq.fromList backups
    , serverRetrySettings = retrySettings
    }

-- | Get a server from the pool.
activeServer :: IORef ServerPool -> IO Server
activeServer ref = do
  ServerPool { serverActive } <- readIORef ref
  return serverActive

-- | Move the current server to the backup pool and pick one of the backup
-- server as the new active server. Currently the scheduler works in
-- round-robin fashion.
failover :: IORef ServerPool -> IO ()
failover ref = atomicModifyIORef' ref $ \pool@ServerPool {..} ->
  case Seq.viewl serverBackup of
    EmptyL -> (pool, ())
    active :< rest -> (newPool, ())
      where
        newPool = pool
          { serverActive = active
          , serverBackup = rest |> serverActive
          }

-----------------------------------------------------------
-- Aeson instances

deriveFromJSON (stripPrefixOptions "database") ''Database
deriveFromJSON (stripPrefixOptions "admin") ''Admin
deriveFromJSON (stripPrefixOptions "user") ''User
deriveFromJSON (stripPrefixOptions "ping") ''Ping
deriveFromJSON (stripPrefixOptions "continuousQuery") ''ContinuousQuery
