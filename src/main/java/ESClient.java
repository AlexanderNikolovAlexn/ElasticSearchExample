import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class ESClient {

    static final int BULKRECORDS = 1000;

    private Client client;
    private IndicesAdminClient adminClient;

    public ESClient() {
        this.client = null;
        adminClient = null;
    }

    public ESClient(String hostName, int portNumber, String clusterName) {
        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", clusterName).build();
            this.client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), portNumber));
            adminClient = this.client.admin().indices();
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public boolean indexExists(String indexName) {
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = adminClient.prepareExists(indexName).execute().actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }


    public boolean deleteIndex(String indexName)
    {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        try {
            DeleteIndexResponse response = adminClient.delete(request).actionGet();
            if (!response.isAcknowledged()) {
                return false;
            }
            return true;
        }
        catch (org.elasticsearch.index.IndexNotFoundException e) {
            return false;
        }
    }

    public boolean createIndex(String indexName)
    {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        CreateIndexResponse response = this.adminClient.create(request).actionGet();
        if (!response.isAcknowledged()) {
            return false;
        }
        return true;
    }

    public boolean addJSON(String indexName, String indexType, String docId, JsonInterface json) {
        IndexRequest indexRequest = new IndexRequest(indexName, indexType, docId);
        indexRequest.source(json.toJson());
        IndexResponse response = client.index(indexRequest).actionGet();
        return response.isCreated();
    }

    public long bulkInsert(String indexName, String indexType, List<JsonInterface> jsons) {
        BulkRequestBuilder bulkBuilder = client.prepareBulk();
        long insertedRecords = 0;
        long failedRecords = 0;
        long bulkRecords = 0;
        String id = "";
        for (int i = 0; i < jsons.size(); i++) {
            JsonInterface json = jsons.get(i);
            String s = json.toJson();
            bulkBuilder.add(client.prepareIndex(indexName, indexType, String.valueOf(i)).setSource(s));
            bulkRecords++;
            if(bulkRecords == BULKRECORDS || i == jsons.size() - 1) {
                BulkResponse bulkResponse = bulkBuilder.execute().actionGet();
                if(bulkResponse.hasFailures()) {
                    failedRecords += bulkResponse.getItems().length;
                }
                System.out.println("Bulk executed!" + bulkResponse.hasFailures() + " Records: " + bulkRecords);
                insertedRecords += bulkRecords;
                bulkRecords = 0;
                bulkBuilder = client.prepareBulk();
            }
        }

        return insertedRecords;
    }

    public Client getClient(){
        return this.client;
    }

}
