import com.google.gson.Gson;

public class MyFile implements JsonInterface {

    private String fileName;
    private String fileExtention;
    private String directoryName;

    public MyFile(String fileName, String fileExtention, String directoryName) {
        this.fileName = fileName;
        this.fileExtention = fileExtention;
        this.directoryName = directoryName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileExtention() {
        return fileExtention;
    }

    public void setFileExtention(String fileExtention) {
        this.fileExtention = fileExtention;
    }

    public String getDirectoryName() {
        return directoryName;
    }

    public void setDirectoryName(String directoryName) {
        this.directoryName = directoryName;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "MyFile{" +
                "fileName='" + fileName + '\'' +
                ", fileExtention='" + fileExtention + '\'' +
                ", directoryName='" + directoryName + '\'' +
                '}';
    }
}
