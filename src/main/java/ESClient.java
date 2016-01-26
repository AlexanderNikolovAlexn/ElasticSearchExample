import com.google.gson.Gson;
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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

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
                insertedRecords += bulkRecords;
                bulkRecords = 0;
                bulkBuilder = client.prepareBulk();
            }
        }

        return insertedRecords;
    }

    public List<JsonInterface> getJsonFilter(String indexName, String indexType, Map<String, String> filter) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (Map.Entry<String, String> entry : filter.entrySet()){
            boolQuery.must(QueryBuilders.matchPhrasePrefixQuery(entry.getKey(), entry.getValue().toLowerCase()));
        }
        SearchResponse response = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(boolQuery)
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();

        List<JsonInterface> myFiles = new ArrayList<JsonInterface>();
        SearchHit[] results = response.getHits().getHits();
        for (SearchHit hit : results) {
            System.out.println("------------------------------");
            Map<String,Object> result = hit.getSource();
            Gson gson = new Gson();
            myFiles.add(gson.fromJson(hit.getSourceAsString(), MyFile.class));
        }

        return myFiles;
    }

    public List<JsonInterface> getJsonMatchAll(String indexName, String type) {

        SearchResponse response = client.prepareSearch(indexName)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(matchAllQuery())
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();

        List<JsonInterface> myFiles = new ArrayList<JsonInterface>();
        SearchHit[] results = response.getHits().getHits();
        for (SearchHit hit : results) {
            System.out.println("------------------------------");
            Map<String,Object> result = hit.getSource();
            System.out.println(result);
            Gson gson = new Gson();
            myFiles.add(gson.fromJson(hit.getSourceAsString(), MyFile.class));
        }

        return myFiles;
    }

    public Client getClient(){
        return this.client;
    }

}
