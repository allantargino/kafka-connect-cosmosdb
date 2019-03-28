package source;

import com.microsoft.azure.cosmosdb.rx._;
import com.microsoft.azure.cosmosdb._;

class DocumentInserter(asyncClient: AsyncDocumentClient, databaseName: String, collectionName: String) {
  def client() = asyncClient
  def database() = databaseName
  def collection() = collectionName

  def insertRandom(n:Int){
      var i = 0;
      for(i <- 1 to n){
          def uuid = java.util.UUID.randomUUID.toString
          insert(uuid, i)
      }
  }

  def insert(id:String, index:Int){
      val doc = new Document("{ 'id': '%s', 'index': '%d'}".format(id, index))
      val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)
      val createDocumentObservable = asyncClient.createDocument(collectionLink, doc, null, false)
      
      createDocumentObservable
            .single()
            .subscribe(
                documentResourceResponse => {
                    println("inserted: " + documentResourceResponse.getActivityId());
                },
                error => {
                    println("an error happened: " + error.getMessage());
                })
  }
}