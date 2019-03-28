package source

import java.util.concurrent.CountDownLatch
import com.microsoft.azure.cosmosdb.rx._
import com.microsoft.azure.cosmosdb._
import com.google.gson._


class PartitionFeedStateManager(asyncClient: AsyncDocumentClient, databaseName: String, collectionName: String) {

  def save(partitionFeedState: PartitionFeedState): Unit = {
    val gson = new Gson()
    val json = gson.toJson(partitionFeedState)
    val document = new Document(json)
    val collectionLink = DocumentClientBuilder.getCollectionLink(databaseName, collectionName)

    val createDocumentObservable = asyncClient.upsertDocument(collectionLink, document, null, false)
    val saveStateCompletionLatch = new CountDownLatch(1)

    createDocumentObservable
      .subscribe(
        documentResourceResponse => {
          println("Saved state for %s with token %s".format(partitionFeedState.id, partitionFeedState.continuationToken))
        },
        error => {
          println("An error happened when saving: " + error.getMessage());
        },
        () => {
          println("End saving")
          //saveStateCompletionLatch.countDown()
        })

    //saveStateCompletionLatch.await()
  }

  def load(partitionKeyRangeId: String): PartitionFeedState = {
    val databaseLink = DocumentClientBuilder.getDatabaseLink(databaseName)
    val querySpec = new SqlQuerySpec("SELECT * FROM @collectionName, where @collectionName.partitionKeyRangeId = @partitionKeyRangeId",
      new SqlParameterCollection(
        new SqlParameter("@collectionName", collectionName),
        new SqlParameter("@partitionKeyRangeId", partitionKeyRangeId)
      ))
    val queryFeedObservable = asyncClient.queryCollections(databaseLink, querySpec, null)

    try {
      val results = queryFeedObservable.toBlocking().single().getResults()
      val partitionFeedState = results.iterator().next()
      return partitionFeedState.toObject(classOf[PartitionFeedState])
    }
    catch {
      case _: Throwable => return new PartitionFeedState(partitionKeyRangeId)
    }
  }
}
