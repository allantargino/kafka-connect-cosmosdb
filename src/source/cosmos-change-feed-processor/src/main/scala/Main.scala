import com.microsoft.azure.cosmosdb.rx._;
import com.microsoft.azure.cosmosdb._;

object Main {

  def success(documentResourceResponse: ResourceResponse[Document]){
    System.out.println(documentResourceResponse.getRequestCharge());
  }

  def main(args: Array[String]) {
    println("Start!")

    val cosmosServiceEndpoint = sys.env("COSMOS_SERVICE_ENDPOINT")
    val cosmosKey = sys.env("COSMOS_KEY")

    val policy = new ConnectionPolicy()
    policy.setConnectionMode(ConnectionMode.Direct)

    val asyncClient = new AsyncDocumentClient.Builder()
				.withServiceEndpoint(cosmosServiceEndpoint)
				.withMasterKeyOrResourceToken(cosmosKey)
				.withConnectionPolicy(policy)
				.withConsistencyLevel(ConsistencyLevel.Eventual)
				.build()

    val doc = new Document("{ 'id': 'doc%d', 'counter': '%d'}".format(2,1))
    val databaseName = "database"
    val collectionName = "collection1"
    val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)

    val createDocumentObservable = asyncClient.createDocument(collectionLink, doc, null, false);

    createDocumentObservable
	            .single()
              .subscribe(
	                documentResourceResponse => {
	                    println(documentResourceResponse.getActivityId());
	                },
	                error => {
	                    println("an error happened: " + error.getMessage());
	                });

    // println("Bye, cosmos!")
    // System.exit(0)
  }
}