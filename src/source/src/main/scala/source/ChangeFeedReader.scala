package source

class ChangeFeedReader(cosmosServiceEndpoint: String, cosmosKey: String, databaseName: String, monitoredCollectionName: String, stateCollectionName: String) {

  val asyncClient = DocumentClientBuilder.buildAsyncDocumentClient(cosmosServiceEndpoint, cosmosKey)
  val partitionFeedStateManager = new PartitionFeedStateManager(asyncClient, databaseName, stateCollectionName)

  val partitionKeyRangeId = "0"
  val partitionInitialFeedState = partitionFeedStateManager.load(partitionKeyRangeId) //TODO: Inject into PartitionFeedReader
  val feedReader = new PartitionFeedReader(asyncClient, databaseName, monitoredCollectionName, partitionKeyRangeId, partitionInitialFeedState, partitionFeedStateManager)


  def readChangeFeed(documentProcessor: List[String] => Unit): Unit = {
    println("Started!")
    println("Initial continuationToken: " + partitionInitialFeedState.continuationToken)
    feedReader.readChangeFeed(documentProcessor)
    println("Finished!")
  }

}
