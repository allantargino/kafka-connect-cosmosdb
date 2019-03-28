package source;

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

    def readChangeFeed(partitionKeyRangeId: String, documentProcessor: List[String] => Unit) {
        val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)
        val changeFeedOptions = new ChangeFeedOptions()
        changeFeedOptions.setPartitionKeyRangeId(partitionKeyRangeId)
        //   changeFeedOptions.setStartFromBeginning(true);
        changeFeedOptions.setRequestContinuation("12");
        changeFeedOptions.setMaxItemCount(100);

        val changeFeedObservable = asyncClient.queryDocumentChangeFeed(collectionLink,changeFeedOptions)
        
        changeFeedObservable
                .doOnNext(value => {
                    val documents = value.getResults().map(d => d.toJson())
                    documentProcessor(documents.toList)
                })
                .subscribe(
                    feedResponse => {
                        println("Count: " + feedResponse.getResults().length)
                        val responseContinuation = feedResponse.getResponseContinuation()
                        println("ResponseContinuation: " + responseContinuation)
                    },
                    error => {
                        println("an error happened: " + error.getMessage());
                    });
                    // TODO: onCompleted
        // changeFeedObservable.toCompletable().await()
        // println("End polling")
  }
}