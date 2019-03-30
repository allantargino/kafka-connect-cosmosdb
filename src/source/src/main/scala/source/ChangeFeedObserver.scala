package source

trait ChangeFeedObserver {
  def processChanges(documentList: List[String])
}
