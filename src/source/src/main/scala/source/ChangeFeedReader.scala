package source

import com.microsoft.azure.cosmosdb.rx._
import com.microsoft.azure.cosmosdb._

import scala.collection.JavaConversions._

class ChangeFeedReader(cosmosServiceEndpoint: String, cosmosKey: String, databaseName: String, monitoredCollectionName: String, stateCollectionName: String) {

  val asyncClient = DocumentClientBuilder.buildAsyncDocumentClient(cosmosServiceEndpoint, cosmosKey)
  val partitionFeedStateManager = new PartitionFeedStateManager(asyncClient, databaseName, stateCollectionName)
  val partitionFeedReaders = createPartitionMap()

  def createPartitionMap(): Map[String, PartitionFeedReader] = {
    val rangeIdList = getPartitionRangeIds()
    val feedReaderMap = Map(rangeIdList map { id => (id, new PartitionFeedReader(asyncClient, databaseName, monitoredCollectionName, id, partitionFeedStateManager)) }: _*)
    return feedReaderMap
  }

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
    for ((id, pfr) <- partitionFeedReaders) pfr.readChangeFeed(documentProcessor)
    //TODO: use latch to await
    println("Finished!")
  }

  def getLocalHostName() = java.net.InetAddress.getLocalHost().getHostName()

}
