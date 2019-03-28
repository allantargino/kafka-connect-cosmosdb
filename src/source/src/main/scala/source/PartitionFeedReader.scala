package source

import com.microsoft.azure.cosmosdb.rx._
import com.microsoft.azure.cosmosdb._

import scala.collection.JavaConversions._

class PartitionFeedReader(asyncClient: AsyncDocumentClient, databaseName: String, collectionName: String, partitionKeyRangeId: String, private var partitionFeedState: PartitionFeedState) {

  /*    def readPartitionKeyRanges(){
        val localhostname = java.net.InetAddress.getLocalHost().getHostName()
        println("localhostname:" + localhostname)

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
      }*/

  private def createChangeFeedOptionsFromState(): ChangeFeedOptions = {
    val changeFeedOptions = new ChangeFeedOptions()
    changeFeedOptions.setPartitionKeyRangeId(partitionKeyRangeId)
    changeFeedOptions.setMaxItemCount(3)

    partitionFeedState.continuationToken match {
      case Some(t) => changeFeedOptions.setRequestContinuation(t)
      case None => changeFeedOptions.setStartFromBeginning(true)
      case null => changeFeedOptions.setStartFromBeginning(true)
    }

    return changeFeedOptions
  }

  def readChangeFeed(documentProcessor: List[String] => Unit) : PartitionFeedState = {
    val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)
    val changeFeedOptions = createChangeFeedOptionsFromState()
    val changeFeedObservable = asyncClient.queryDocumentChangeFeed(collectionLink, changeFeedOptions)

    changeFeedObservable
      .doOnNext(value => {
        val documents = value.getResults().map(d => d.toJson())
        documentProcessor(documents.toList)
      })
      .subscribe(
        feedResponse => {
          partitionFeedState = new PartitionFeedState(partitionKeyRangeId, Option(feedResponse.getResponseContinuation()))

          println("Count: " + feedResponse.getResults().length)
          println("ResponseContinuation: " + feedResponse.getResponseContinuation())
        },
        error => {
          println("an error happened: " + error.getMessage());
        },
        () => {
          println("Finished reading change feed from partitionKeyRangeId" + partitionKeyRangeId)
        })

    changeFeedObservable.toCompletable().await()

    return partitionFeedState
  }
}