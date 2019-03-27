import com.microsoft.azure.cosmosdb.rx._;
import com.microsoft.azure.cosmosdb._;

object Main {

  def documentProcessor(documentList: List[String]) {
    if(documentList.length > 0){
      println("Documents to process:" + documentList.length)
      documentList.foreach { println }
    }else{
      println("No documents to process.")
    }
  }

  def main(args: Array[String]) {
    val cosmosServiceEndpoint = sys.env("COSMOS_SERVICE_ENDPOINT")
    val cosmosKey = sys.env("COSMOS_KEY")
    val databaseName = "database"
    val collectionName = "collection1"

    val localhostname = java.net.InetAddress.getLocalHost().getHostName()
    println("localhostname:" + localhostname)


    println("Start, main!")
    val asyncClient = ClientBuilder.buildAsyncDocumentClient(cosmosServiceEndpoint, cosmosKey)
    // val documentInserter = new DocumentInserter(asyncClient, databaseName, collectionName)
    val feedReader = new ChangeFeedReader(asyncClient, databaseName, collectionName)

    // documentInserter.insertRandom(3)
    // feedListener.readPartitionKeyRanges()
    feedReader.readChangeFeed("0", documentProcessor)

    println("Bye, main!")
  }
}