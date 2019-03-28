package source

import com.microsoft.azure.cosmosdb.rx._
import com.microsoft.azure.cosmosdb._

import scala.collection.JavaConversions._

class PartitionFeedReader(asyncClient: AsyncDocumentClient, databaseName: String, collectionName: String, partitionKeyRangeId: String, private var partitionFeedState: PartitionFeedState, partitionFeedStateManager: PartitionFeedStateManager) {

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
      case null => changeFeedOptions.setStartFromBeginning(true)
      case "" => changeFeedOptions.setStartFromBeginning(true)
      case t => changeFeedOptions.setRequestContinuation(t)
    }

    return changeFeedOptions
  }

  def readChangeFeed(documentProcessor: List[String] => Unit)  {
    val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)
    val changeFeedOptions = createChangeFeedOptionsFromState()
    val changeFeedObservable = asyncClient.queryDocumentChangeFeed(collectionLink, changeFeedOptions)

    changeFeedObservable
      .doOnNext(feedResponse => {
        val documents = feedResponse.getResults().map(d => d.toJson())
        documentProcessor(documents.toList)
      })
      .
      .doOnNext(feedResponse => {
        val continuationToken = feedResponse.getResponseContinuation().replaceAll("^\"|\"$", "")
        partitionFeedState = new PartitionFeedState(partitionKeyRangeId, continuationToken)
        partitionFeedStateManager.save(partitionFeedState)
      })
      .subscribe(
        feedResponse => {
          println("Count: " + feedResponse.getResults().length)
          println("ResponseContinuation: " + feedResponse.getResponseContinuation())
        },
        error => {
          println("an error happened: " + error.getMessage())
        },
        () => {
          println("Finished reading change feed from partitionKeyRangeId" + partitionKeyRangeId)
        })
  }
}