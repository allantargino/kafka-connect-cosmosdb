package source

object Main {

  // ChangeFeed Processor:
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
    // Parameters:
    val cosmosServiceEndpoint = sys.env("COSMOS_SERVICE_ENDPOINT")
    val cosmosKey = sys.env("COSMOS_KEY")
    val databaseName = "database"
    val monitoredCollectionName = "collection1"
    val stateCollectionName = "collectionAux1"

    // Read ChangeFeed:
    val changeFeedReader = new ChangeFeedReader(cosmosServiceEndpoint, cosmosKey, databaseName, monitoredCollectionName, stateCollectionName)
    changeFeedReader.readChangeFeed(documentProcessor)
    System.exit(0)
  }
}
