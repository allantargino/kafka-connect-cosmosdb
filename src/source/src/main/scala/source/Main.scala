package source

object Main {

  def documentProcessor(documentList: List[String]) {
    if(documentList.nonEmpty){
      println("Documents to process:" + documentList.length)
      documentList.foreach { println }
    }else{
      println("No documents to process.")
    }
  }

  def main(args: Array[String]) {
    println("Started!")

    val cosmosServiceEndpoint = sys.env("COSMOS_SERVICE_ENDPOINT")
    val cosmosKey = sys.env("COSMOS_KEY")
    val databaseName = "database"
    val collectionName = "collection1"
    val partitionKeyRangeId = "0"

    val asyncClient = ClientBuilder.buildAsyncDocumentClient(cosmosServiceEndpoint, cosmosKey)
    val partitionInitialFeedState = new PartitionFeedState(partitionKeyRangeId, Option("11"))
    val feedReader = new PartitionFeedReader(asyncClient, databaseName, collectionName, partitionKeyRangeId, partitionInitialFeedState)

    val partitionFinalFeedState = feedReader.readChangeFeed(documentProcessor)

    println("Finished! Last token: " + partitionFinalFeedState.continuationToken)
  }
}
