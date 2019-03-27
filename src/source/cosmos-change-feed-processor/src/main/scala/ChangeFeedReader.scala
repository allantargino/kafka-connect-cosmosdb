import com.microsoft.azure.cosmosdb.rx._;
import com.microsoft.azure.cosmosdb._;
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class ChangeFeedReader(asyncClient: AsyncDocumentClient, databaseName: String, collectionName: String) {
    
    def readPartitionKeyRanges(){
        val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)
        val feedOptions = new FeedOptions()
        val changeFeedObservable = asyncClient.readPartitionKeyRanges(collectionLink,feedOptions)
      
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


    def readChangeFeed(partitionKeyRangeId: String):List[com.microsoft.azure.cosmosdb.Document] = {
        val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)
        val changeFeedOptions = new ChangeFeedOptions()
        changeFeedOptions.setPartitionKeyRangeId(partitionKeyRangeId)
        //   changeFeedOptions.setStartFromBeginning(true);
        changeFeedOptions.setRequestContinuation("12");
        changeFeedOptions.setMaxItemCount(2);

        var documents = new ListBuffer[com.microsoft.azure.cosmosdb.Document]()

        val changeFeedObservable = asyncClient.queryDocumentChangeFeed(collectionLink,changeFeedOptions)
        
        changeFeedObservable
                .subscribe(
                    feedResponse => {
                        val results = feedResponse.getResults()
                        println("feedResponse: " + results.length)
                        // results.foreach { println }
                        results.foreach(documents += _)

                        val responseContinuation = feedResponse.getResponseContinuation()
                        println("responseContinuation: " + responseContinuation)
                    },
                    error => {
                        println("an error happened: " + error.getMessage());
                    });

        changeFeedObservable.toCompletable().await()

        println("End polling")
        documents.toList
  }
}