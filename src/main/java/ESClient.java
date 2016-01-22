import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESClient {

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

    public boolean addJSON(String indexName, String indexType, String docId, String json) {
        IndexRequest indexRequest = new IndexRequest(indexName, indexType, docId);
        indexRequest.source(json);
        IndexResponse response = client.index(indexRequest).actionGet();
        return response.isCreated();
    }

    public Client getClient(){
        return this.client;
    }

}
