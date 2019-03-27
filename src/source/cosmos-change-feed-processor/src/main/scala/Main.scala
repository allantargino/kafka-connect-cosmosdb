import com.microsoft.azure.cosmosdb.rx._;
import com.microsoft.azure.cosmosdb._;

object Main {

  def process() {

  }

  def main(args: Array[String]) {
    val cosmosServiceEndpoint = sys.env("COSMOS_SERVICE_ENDPOINT")
    val cosmosKey = sys.env("COSMOS_KEY")
    val databaseName = "database"
    val collectionName = "collection1"

    val localhostname = java.net.InetAddress.getLocalHost().getHostName()
    println("localhostname:" + localhostname)


    println("Start!")
    val asyncClient = ClientBuilder.buildAsyncDocumentClient(cosmosServiceEndpoint, cosmosKey)
    // val documentInserter = new DocumentInserter(asyncClient, databaseName, collectionName)
    val feedReader = new ChangeFeedReader(asyncClient, databaseName, collectionName)

    // documentInserter.insertRandom(3)
    // feedListener.readPartitionKeyRanges()
    val documentsChanged = feedReader.readChangeFeed("0")
    // feedReader.readChangeFeed("0")
    println("Finished polling. Printing:")
    documentsChanged.foreach { println }

    println("Bye, cosmos!")
    // System.exit(0)
  }
}