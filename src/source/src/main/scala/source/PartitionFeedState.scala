package source

class PartitionFeedState(val partitionKeyRangeId: String, val continuationToken: Option[String]) {
  def this(partitionKeyRangeId: String) = this(partitionKeyRangeId, None)
}