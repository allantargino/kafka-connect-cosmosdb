package source

import com.microsoft.azure.cosmosdb.rx._
import com.microsoft.azure.cosmosdb._

import scala.collection.JavaConversions._
import scala.collection.mutable.LinkedList

class ChangeFeedReader(cosmosServiceEndpoint: String, cosmosKey: String, databaseName: String, monitoredCollectionName: String, stateCollectionName: String) {

  //TODO: Create a map rangeId->feedReader

  val asyncClient = DocumentClientBuilder.buildAsyncDocumentClient(cosmosServiceEndpoint, cosmosKey)
  val partitionFeedStateManager = new PartitionFeedStateManager(asyncClient, databaseName, stateCollectionName)

  val partitionKeyRangeId = "0"
  val partitionInitialFeedState = partitionFeedStateManager.load(partitionKeyRangeId) //TODO: Inject into PartitionFeedReader
  val feedReader0 = new PartitionFeedReader(asyncClient, databaseName, monitoredCollectionName, partitionKeyRangeId, partitionInitialFeedState, partitionFeedStateManager)

  def getPartitionRangeIds(): List[String] = {
    val collectionLink = DocumentClientBuilder.getCollectionLink(databaseName, monitoredCollectionName)
    val feedOptions = new FeedOptions()
    val changeFeedObservable = asyncClient.readPartitionKeyRanges(collectionLink, null)

    var results = List[PartitionKeyRange]()
    changeFeedObservable.toBlocking().forEach(x => results = results ++ x.getResults())

    return results.map(p => p.getId)
  }

  def readChangeFeed(documentProcessor: List[String] => Unit): Unit = {
    println("Started!")
    println("Initial continuationToken: " + partitionInitialFeedState.continuationToken)
    //TODO: foreach feedReader:
    feedReader0.readChangeFeed(documentProcessor)
    //TODO: use latch to await
    println("Finished!")
  }

  def getLocalHostName() = java.net.InetAddress.getLocalHost().getHostName()

}
