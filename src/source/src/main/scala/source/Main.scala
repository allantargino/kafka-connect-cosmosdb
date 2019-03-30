package source

object Main {

  class SampleObserver extends ChangeFeedObserver{
    override def processChanges(documentList: List[String]): Unit = {
      if (documentList.nonEmpty) {
        println("Documents to process:" + documentList.length)
        documentList.foreach {
          println
        }
      } else {
        println("No documents to process.")
      }
    }
  }

  def main(args: Array[String]) {
    val uri = sys.env("COSMOS_SERVICE_ENDPOINT")
    val masterKey = sys.env("COSMOS_KEY")
    val databaseName = "database"
    val monitoredCollectionName = "collection1"
    val stateCollectionName = "collectionAux1"

    val feedCollectionInfo = new DocumentCollectionInfo(uri, masterKey, databaseName, monitoredCollectionName)
    val leaseCollectionInfo = new DocumentCollectionInfo(uri, masterKey, databaseName, stateCollectionName)
    val changeFeedProcessorOptions = new ChangeFeedProcessorOptions()
    val sampleObserver = new SampleObserver()

    val builder = new ChangeFeedProcessorBuilder()
    val processor =
      builder
        .withFeedCollection(feedCollectionInfo)
        .withLeaseCollection(leaseCollectionInfo)
        .withProcessorOptions(changeFeedProcessorOptions)
        .withObserver(sampleObserver)
        .build()

    processor.start()
    System.exit(0)
  }
}
