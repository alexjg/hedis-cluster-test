{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module Main where

import Database.Redis
import Data.Bifunctor(bimap)
import Data.ByteString(ByteString)
import qualified Data.Text as T
import qualified Data.ByteString.Char8 as Char8
import Data.Maybe(listToMaybe)
import Control.Monad(forM_, void)
import Control.Monad.IO.Class(liftIO)
import Control.Monad.Except(ExceptT, runExceptT, liftEither, throwError)


data Node = Node
    { conn ::  Connection
    , host :: String
    , port :: Int
    , nodeId :: ByteString
    }

instance Show Node where
    show Node{..} = "Node{host = " <> show host <> ", port = " <> show port <> ", id = " <> show nodeId <> "}"

type HedisTest a = ExceptT T.Text IO a

main :: IO ()
main = do
    let connInfo = defaultConnectInfo { connectPort = PortNumber 7000 }
    conn <- connectCluster connInfo
    _ <- runRedis conn $ set "somekey" "someval"
    migrateResult <- runExceptT $ migrateHashSlotWithInterstitialAction conn (keyToSlot "somekey") (liftIO $ runRedis conn $ get "somekey")
    --migrateResult <- runExceptT $ migrateHashSlotWithInterstitialAction conn (keyToSlot "somekey") (liftIO $ void getLine)
    case migrateResult of
        Left e -> putStrLn $ "Error running migration " <> T.unpack e
        Right interstitialResult -> do
            putStrLn $ "Result of 'GET somekey' after migration but before CLUSTER SETSLOT NODE: " <> show interstitialResult
            r <- runRedis conn $ get "somekey"
            putStrLn $ "Result of 'GET somekey' after CLUSTER SETSLOT node: " <> show r
    putStrLn "done"



migrateHashSlot :: Connection -> HashSlot -> HedisTest ()
migrateHashSlot clusterConn slot = migrateHashSlotWithInterstitialAction clusterConn slot (return ())

-- | Migrates the given hashslot to a new node. An action can be specified to be run after the keys in the hashslot are migrated but before the slots are marked as finished migrating/importing.
--
--   For example, say we have a database with two nodes and a single key 'somekey' -> 'someval', with 'somekey' in hashslot 1. The database looks like this:
--
--   Node | Hashslot      | Keys
--   -------------------------------
--   node1| 1-10000       | somekey
--   node2| 100001 - 16384| 
--
--
--   Calling this function with hashslot `1` will issue the following commands 
--
--   node1 > CLUSTER SETSLOT 1 MIGRATING node2
--   node2 > CLUSTER SETSLOT 1 IMPORTING node1
--   node1 > MIGRATE node2 7000 somekey 0 1000
--   -- Now run the interstitial action
--   node1 > CLUSTER SETSLOT 1 NODE node2
--   node2 > CLUSTER SETSLOT 2 NODE node2
--   return the result of the interstitial action
migrateHashSlotWithInterstitialAction 
    :: Connection -- ^ A non clustered connection to one of the nodes in the cluster
    -> HashSlot -- ^ The hashslot to migrate
    -> HedisTest a  -- ^ An action to run after migrating the key but before marking the migration complete
    -> HedisTest a
migrateHashSlotWithInterstitialAction clusterConn slot action = do
    slotsReply <- liftIO $ runRedis clusterConn clusterSlots
    case slotsReply of
        Left e -> fail $ show e
        Right slots -> do
            masterNode <- masterNodeForHashSlot slots slot
            otherMasterNode <- masterForOtherNode slots masterNode
            keysInSlot <- runRedisWithError (conn masterNode) $ clusterGetKeysInSlot (toInteger slot) 1000
            liftIO $ putStrLn $ "Moving " <> show keysInSlot <> " from " <> show masterNode <> " to " <> show otherMasterNode
            runRedisWithError (conn masterNode) $ clusterSetSlotMigrating (toInteger slot) (nodeId otherMasterNode)
            runRedisWithError (conn otherMasterNode) $ clusterSetSlotImporting (toInteger slot) (nodeId masterNode)
            forM_ keysInSlot (\k -> runRedisWithError (conn masterNode) $ migrate (Char8.pack $ host otherMasterNode) (Char8.pack $ show $ port otherMasterNode) k 0 1000)
            result <- action
            runRedisWithError (conn masterNode) $ clusterSetSlotNode (toInteger slot) (nodeId otherMasterNode)
            runRedisWithError (conn otherMasterNode) $ clusterSetSlotNode (toInteger slot) (nodeId otherMasterNode)
            return result

runRedisWithError :: Connection -> Redis (Either Reply a) -> HedisTest a
runRedisWithError conn redis = liftIO (runRedis conn redis) >>= (liftEither . bimap (T.pack . show) id)

masterNodeForHashSlot :: ClusterSlotsResponse -> HashSlot -> HedisTest Node
masterNodeForHashSlot clusterSlots slot = nodeMatchingFilter clusterSlots ("Unable to find node matching slot " <> T.pack (show slot)) (isForHashSlot slot)

masterForOtherNode :: ClusterSlotsResponse -> Node -> HedisTest Node
masterForOtherNode clusterSlots Node{..} = nodeMatchingFilter clusterSlots ("Unable to find master without id " <> T.pack (show nodeId)) (not . hasNodeId nodeId)


nodeMatchingFilter :: ClusterSlotsResponse -> T.Text -> (ClusterSlotsResponseEntry -> Bool) -> HedisTest Node
nodeMatchingFilter ClusterSlotsResponse{..} errMessage entryFilter = do
    let slotResponses = filter entryFilter clusterSlotsResponseEntries
    case slotResponses of
        [] -> throwError errMessage
        (ClusterSlotsResponseEntry{clusterSlotsResponseEntryMaster=ClusterSlotsNode{..}}:_) -> do
            let host = Char8.unpack clusterSlotsNodeIP
            let port = clusterSlotsNodePort
            let nodeId = clusterSlotsNodeID
            conn <- liftIO $ connect defaultConnectInfo { connectPort = PortNumber $ toEnum clusterSlotsNodePort, connectHost = Char8.unpack clusterSlotsNodeIP }
            return $ Node{..}


isForHashSlot :: HashSlot -> ClusterSlotsResponseEntry -> Bool
isForHashSlot slot ClusterSlotsResponseEntry{..} = (clusterSlotsResponseEntryStartSlot <= slotInt) && (clusterSlotsResponseEntryEndSlot >= slotInt) where
    slotInt :: Int
    slotInt = fromIntegral slot

hasNodeId :: ByteString -> ClusterSlotsResponseEntry -> Bool
hasNodeId thisNodeId ClusterSlotsResponseEntry{clusterSlotsResponseEntryMaster=ClusterSlotsNode{..}} = thisNodeId == clusterSlotsNodeID
