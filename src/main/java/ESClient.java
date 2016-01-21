import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESClient {

    private Client client;

    public ESClient() {
        this.client = null;
    }

    public ESClient(String hostName, int portNumber, String clusterName) {
        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", clusterName).build();
            this.client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), portNumber));
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public Client getClient(){
        return this.client;
    }

}
