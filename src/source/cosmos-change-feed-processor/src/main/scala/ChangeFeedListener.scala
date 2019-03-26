import com.microsoft.azure.cosmosdb.rx._;
import com.microsoft.azure.cosmosdb._;
import scala.collection.JavaConversions._

class ChangeFeedListener(asyncClient: AsyncDocumentClient, databaseName: String, collectionName: String) {
    
    def listen(){
      val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)
      val changeFeedOptions = new ChangeFeedOptions()
      changeFeedOptions.setPartitionKeyRangeId("0")
      changeFeedOptions.setStartFromBeginning(true);
      val changeFeedObservable = asyncClient.queryDocumentChangeFeed(collectionLink,changeFeedOptions)

    //   val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)
    //   val feedOptions = new FeedOptions()
    //   val changeFeedObservable = asyncClient.readPartitionKeyRanges(collectionLink,feedOptions)
      
      changeFeedObservable
            .subscribe(
                feedResponse => {
                    val documents = feedResponse.getResults()
                    println("feedResponse: " + documents.length)
                    documents.foreach { println }
                },
                error => {
                    println("an error happened: " + error.getMessage());
                });
  }
}