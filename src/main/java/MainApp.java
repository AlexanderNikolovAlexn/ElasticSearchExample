import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class MainApp {

    static final String HOSTNAME = "localhost";
    static final int PORTNUMBER = 9300;
    static final String CLUSTERNAME = "alex";

    public static void main(String[] args) {

        System.out.println("Starting here!");

        String indexName = "customer";
        String docType = "external";
        String docId = "25";

        ESClient esClient = new ESClient(HOSTNAME, PORTNUMBER, CLUSTERNAME);
        Client client = esClient.getClient();

        IndexResponse response = null;

        try {
            response = client.prepareIndex(indexName, docType, docId)
                    .setSource(XContentFactory.jsonBuilder()
                                    .startObject()
                                    .field("name", "alex")
                                    .field("age", 28)
                                    .endObject()
                    )
                    .execute()
                    .actionGet();
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            client.close(); //close it down
        }

        System.out.println(response);

        System.out.println("Ending here!");
    }

}
