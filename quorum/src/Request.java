import java.nio.ByteBuffer;

public class Request {
    String requestType = "";
    String filename = "";
    String fileContent = "";
    public Request(String filename, String fileContent){
        this.requestType = "write";
		this.filename = filename;
		this.fileContent = fileContent;
    }
    public Request(String filename){
        this.requestType = "read";
		this.filename = filename;
    }    
}

