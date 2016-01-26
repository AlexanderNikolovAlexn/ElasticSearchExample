import com.google.common.io.Files;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestClass {

    static final String HOSTNAME = "localhost";
    static final int PORTNUMBER = 9300;
    static final String CLUSTERNAME = "alex";

    public static void main(String[] args) {

        String indexName = "testfiles";
        String indexType = "files";
        String dir = "C:\\dev\\eclipse mars";
        List<JsonInterface> myfiles = new ArrayList<JsonInterface>();
        getFiles(dir, myfiles);
        ESClient esClient = new ESClient(HOSTNAME, PORTNUMBER, CLUSTERNAME);

        // insertOneByOne(indexName, indexType, myfiles, esClient);
        // insertBulk(indexType, myfiles, esClient);
        searchText(indexName, indexType, myfiles, esClient);
    }

    private static void searchText(String indexName, String indexType, List<JsonInterface> myfiles, ESClient esClient) {
        long startTime;
        long stopTime;
        double elapsedTimeSeconds;
        double elapsedTimeMiliSeconds;

        Map<String, String> filters = new HashMap<String, String>();
        filters.put("fileExtention", "cla*");
        filters.put("fileName", "ne*");
        System.out.println("Start searching!");

        startTime = System.nanoTime();
        //List<JsonInterface> myFiles = esClient.getJsonMatchAll(indexName, indexType);
        List<JsonInterface> myFiles = esClient.getJsonFilter(indexName, indexType, filters);
        stopTime = System.nanoTime();
        elapsedTimeSeconds = (double) ((stopTime - startTime) / 1000000000);
        elapsedTimeMiliSeconds = (double) ((stopTime - startTime) / 1000000);

        for(JsonInterface myFile: myFiles) {
            System.out.println(myFile.toString());
        }

        System.out.println("Found: " + myfiles.size() + " records. Elapsed time: " + elapsedTimeSeconds + "s(" + elapsedTimeMiliSeconds + "ms)");
    }

    private static void insertBulk(String indexType, List<JsonInterface> myfiles, ESClient esClient) {
        long startTime;
        long stopTime;
        double elapsedTimeSeconds;
        double elapsedTimeMiliSeconds;

        String indexNameBulk = "testfilesbulk";

        startTime = System.nanoTime();

        // create index
        if(esClient.indexExists(indexNameBulk)) {
            esClient.deleteIndex(indexNameBulk);
        }
        esClient.createIndex(indexNameBulk);

        // bulk insert in index
        System.out.println("Start bulk inserting!");
        esClient.bulkInsert(indexNameBulk, indexType, myfiles);

        stopTime = System.nanoTime();
        elapsedTimeSeconds = (double) ((stopTime - startTime) / 1000000000);
        elapsedTimeMiliSeconds = (double) ((stopTime - startTime) / 1000000);
        System.out.println("Bulk inserted: " + myfiles.size() + " Elapsed time: " + elapsedTimeSeconds + "s(" + elapsedTimeMiliSeconds + "ms)");
    }

    private static void insertOneByOne(String indexName, String indexType, List<JsonInterface> myfiles, ESClient esClient) {
        long startTime = System.nanoTime();

        // create index
        if(esClient.indexExists(indexName)) {
            esClient.deleteIndex(indexName);
        }
        esClient.createIndex(indexName);

        // insert in index
        System.out.println("Start one by one inserting!");
        for (int i = 0; i < myfiles.size(); i++) {
            esClient.addJSON(indexName, indexType, String.valueOf(i), myfiles.get(i));
        }

        long stopTime = System.nanoTime();
        double elapsedTimeSeconds = (double) ((stopTime - startTime) / 1000000000);
        double elapsedTimeMiliSeconds = (double) ((stopTime - startTime) / 1000000);
        System.out.println("Inserted: " + myfiles.size() + " Elapsed time: " + elapsedTimeSeconds + "s(" + elapsedTimeMiliSeconds + "ms)");
    }

    public static void getFiles(String directoryName, List<JsonInterface> files) {
        File directory = new File(directoryName);
        File[] fList = directory.listFiles();
        for (File file : fList) {
            if (file.isFile()) {
                MyFile myfile = new MyFile(Files.getNameWithoutExtension(file.getName()), Files.getFileExtension(file.getAbsolutePath()), file.getAbsolutePath());
                files.add(myfile);
            } else if (file.isDirectory()) {
                getFiles(file.getAbsolutePath(), files);
            }
        }
    }
}
