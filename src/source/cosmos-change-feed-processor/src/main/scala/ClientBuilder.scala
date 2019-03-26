import com.microsoft.azure.cosmosdb.rx._;
import com.microsoft.azure.cosmosdb._;

object ClientBuilder {
    
    def createConnectionPolicy():ConnectionPolicy = {
        val policy = new ConnectionPolicy()
        policy.setConnectionMode(ConnectionMode.Direct)
        policy
    }

    def buildAsyncDocumentClient(cosmosServiceEndpoint: String, cosmosKey: String): AsyncDocumentClient = {
       new AsyncDocumentClient.Builder()
                    .withServiceEndpoint(cosmosServiceEndpoint)
                    .withMasterKeyOrResourceToken(cosmosKey)
                    .withConnectionPolicy(createConnectionPolicy())
                    .withConsistencyLevel(ConsistencyLevel.Eventual)
                    .build()
    }
}