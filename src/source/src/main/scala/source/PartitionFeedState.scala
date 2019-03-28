package source

class PartitionFeedState(val id: String, val continuationToken: String) {
  def this(id: String) = this(id, null)
}