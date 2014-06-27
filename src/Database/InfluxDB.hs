module Database.InfluxDB
  (
  -- * Series data types
    Series(..), seriesColumns, seriesPoints
  , SeriesData(..)
  , Value(..)

  -- ** Encoding
  , ToSeriesData(..)
  , ToValue(..)

  -- ** Decoding
  , FromSeries(..), fromSeries
  , FromRow(..), fromSeriesData
  , FromValue(..), fromValue

  , withValues, (.:), (.:?), (.!=)
  , typeMismatch

  -- * HTTP API
  -- ** Data types
  , Config(..), newConfig
  , Credentials(..), rootCreds
  , TimePrecision(..)
  , Server(..), localServer
  , ServerPool, newServerPool, newServerPoolWithRetrySettings
  , Database(..)
  , User(..)
  , Admin(..)
  , Ping(..)

  -- ** Writing Data

  -- *** Updating Points
  , post, postWithPrecision
  , SeriesT, PointT
  , writeSeries
  , withSeries
  , writePoints

  -- *** Deleting Points
  -- **** One Time Deletes
  , deleteSeries
  -- **** Regularly Scheduled Deletes (not implemented)
  -- , getScheduledDeletes
  -- , addScheduledDelete
  -- , removeScheduledDelete

  -- ** Querying Data
  , query
  , Stream(..)
  , queryChunked

  -- ** Administration & Security
  -- *** Creating and Dropping Databases
  , listDatabases
  , createDatabase
  , dropDatabase

  -- *** Continuous queries
  , listContinuousQueries
  , createContinuousQuery
  , deleteContinuousQuery

  -- *** Security
  -- **** Cluster admin
  , listClusterAdmins
  , authenticateClusterAdmin
  , addClusterAdmin
  , updateClusterAdminPassword
  , deleteClusterAdmin
  -- **** Database user
  , listDatabaseUsers
  , authenticateDatabaseUser
  , addDatabaseUser
  , updateDatabaseUserPassword
  , deleteDatabaseUser
  , grantAdminPrivilegeTo
  , revokeAdminPrivilegeFrom

  -- *** Other API
  , ping
  , listInterfaces
  , isInSync
  ) where

import Database.InfluxDB.Decode
import Database.InfluxDB.Encode
import Database.InfluxDB.Http
import Database.InfluxDB.Types
