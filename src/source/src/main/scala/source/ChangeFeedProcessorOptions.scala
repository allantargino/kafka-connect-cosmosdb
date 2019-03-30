package source

class ChangeFeedProcessorOptions(val queryPartitionsMaxBatchSize: Int) {

  def this() = this(100)
}
