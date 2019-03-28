package source

object Main {

  def documentProcessor(documentList: List[String]) {
    if (documentList.nonEmpty) {
      println("Documents to process:" + documentList.length)
      documentList.foreach {
        println
      }
    } else {
      println("No documents to process.")
    }
  }

  def main(args: Array[String]) {
    val cosmosServiceEndpoint = sys.env("COSMOS_SERVICE_ENDPOINT")
    val cosmosKey = sys.env("COSMOS_KEY")
    val databaseName = "database"
    val monitoredCollectionName = "collection1"
    val stateCollectionName = "collectionAux1"
    val partitionKeyRangeId = "0"

    val asyncClient = ClientBuilder.buildAsyncDocumentClient(cosmosServiceEndpoint, cosmosKey)
    val partitionFeedStateManager = new PartitionFeedStateManager(asyncClient, databaseName, stateCollectionName)
    val partitionInitialFeedState = partitionFeedStateManager.load(partitionKeyRangeId)
    val feedReader = new PartitionFeedReader(asyncClient, databaseName, monitoredCollectionName, partitionKeyRangeId, partitionInitialFeedState, partitionFeedStateManager)

    println("Started! Initial continuationToken: " + partitionInitialFeedState.continuationToken)

    val partitionFinalFeedState = feedReader.readChangeFeed(documentProcessor)

    println("Finished! Final continuationToken: " + partitionFinalFeedState.continuationToken)
  }
}
