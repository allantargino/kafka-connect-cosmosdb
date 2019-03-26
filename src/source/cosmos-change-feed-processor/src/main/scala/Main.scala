import com.microsoft.azure.cosmosdb.rx._;
import com.microsoft.azure.cosmosdb._;

object Main {

  def success(documentResourceResponse: ResourceResponse[Document]){
    System.out.println(documentResourceResponse.getRequestCharge());
  }

  def main(args: Array[String]) {
    val cosmosServiceEndpoint = sys.env("COSMOS_SERVICE_ENDPOINT")
    val cosmosKey = sys.env("COSMOS_KEY")
    val databaseName = "database"
    val collectionName = "collection1"

    println("Start!")
    val asyncClient = ClientBuilder.buildAsyncDocumentClient(cosmosServiceEndpoint, cosmosKey)
    val documentInserter = new DocumentInserter(asyncClient, databaseName, collectionName)
    val feedListener = new ChangeFeedListener(asyncClient, databaseName, collectionName)

    // documentInserter.insertRandom(3)
    feedListener.listen()

    // println("Bye, cosmos!")
    // System.exit(0)
  }
}