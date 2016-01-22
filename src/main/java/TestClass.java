import com.google.common.io.Files;

import java.io.File;

public class TestClass {

    public static void main(String[] args) {

        File dir = new File("C:\\dev");
        directory(dir);
        //displayIt(dir);
    }

    public static void directory(File dir) {
        if(dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                System.out.println(Files.getNameWithoutExtension(file.getName()) + "$" + Files.getFileExtension(file.getAbsolutePath()) + "$" + file.getAbsolutePath());
                if (file.listFiles() != null)
                    directory(file);
            }
        }
    }

    public static void displayIt(File node){
        if(node.exists()) {
            System.out.println(node.getAbsoluteFile());

            if (node.isDirectory()) {
                String[] subNote = node.list();
                for (String filename : subNote) {
                    displayIt(new File(node, filename));
                }
            }
        }
    }
}
