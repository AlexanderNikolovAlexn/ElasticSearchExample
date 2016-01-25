import com.google.common.io.Files;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestClass {

    static final String HOSTNAME = "localhost";
    static final int PORTNUMBER = 9300;
    static final String CLUSTERNAME = "alex";

    public static void main(String[] args) {

        String indexName = "testfiles";
        String indexType = "files";
        String dir = "C:\\dev\\tmp";
        List<JsonInterface> myfiles = new ArrayList<JsonInterface>();
        getFiles(dir, myfiles);
        ESClient esClient = new ESClient(HOSTNAME, PORTNUMBER, CLUSTERNAME);

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
