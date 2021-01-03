import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;


import org.apache.thrift.server.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/*import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;*/

public class Client {
	TTransport superNodeTransport;
	TTransport nodeTransport;
	
	SuperNodeService.Client superNodeServer=null;
	NodeService.Client nodeServer=null;
	
	Address superNodeAddress;
	Address nodeAddress;
	
	Scanner sc;
	
	public static void main(String [] args) {
    	if(args.length < 2) {
    	    System.err.println("Usage: java Client serverIP serverPort");
    	    return;
    	}
    	try {
        	Client client = new Client();
        	client.initialize(args);
    	}catch(Exception e) {
    	    e.printStackTrace();
    	}
	}
	
	private void initialize(String [] args) {
    	try {     	
        	superNodeAddress = new Address(args[0], Integer.parseInt(args[1]), -1);   
        	superNodeTransport = new TSocket(superNodeAddress.ip, superNodeAddress.port);
        	boolean connectSuceed = this.connect(superNodeTransport,"SuperNodeService");
        	while (!connectSuceed){
        		System.err.println("Failed to connect to SuperNode Server " + 
        				superNodeAddress.ip + ":" + superNodeAddress.port+", please input "
        						+ "the server ip and server port again: serverIP serverPort");
        		
        		String[] input = sc.nextLine().split(" ");
            	superNodeAddress = new Address(input[0], Integer.parseInt(input[1]), -1);   
            	superNodeTransport = new TSocket(superNodeAddress.ip, superNodeAddress.port);
            	connectSuceed = this.connect(superNodeTransport,"SuperNodeService");
        	}
    		System.out.println("Connected to SuperNode Server " + 
    				superNodeAddress.ip + ":" + superNodeAddress.port);
    		this.start();
    		superNodeTransport.close();
    	}catch(Exception e) {
    	    e.printStackTrace();
    	}		
	}
	
	private void start()throws TException {
		nodeAddress = superNodeServer.GetNode();
    	nodeTransport = new TSocket(nodeAddress.ip, nodeAddress.port);
    	boolean connectSuceed = connect(nodeTransport,"NodeService");	
    	if (!connectSuceed) {
    		System.out.println("Failed to Connect Node Server " + 
    				nodeAddress.ip + ":" + nodeAddress.ip);	  
    		return;
    	}
    	sc = new Scanner(System.in);
		while(true) {
			System.out.println("Input Command: Set Book_title Genre|Set Filename|Get Book_title|Quit");
			String input = sc.nextLine();
			String command = input.split(" ")[0];
			try {
				if (command.equals("Quit")) return;
				else if (command.equals("Set")) this.set(input.substring(5, input.length()-1));
				else if (command.equals("Get")) this.get(input.substring(5, input.length()-1));
				else System.out.println("Wrong Command: please input again");
			}catch(Exception e) {
				System.out.println("Wrong Command: please input again");
			}
		}
	}
	
	public boolean set(String s) {
		try {
			 String[] input = s.split("\" \"");
			if (input.length == 1) {
				String filename = input[0];				
				try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
				    String line;
				    while ((line = br.readLine()) != null) {
				       String[] command = line.split(":");
				       this.set_single_file(command[0], command[1]);
				    }
				    return true;
				}catch(Exception e) {
					System.out.println("Failed to find "+filename);
					return false;
				}
			}else if (input.length==2) {
				this.set_single_file(input[0], input[1]);
			}			
		}catch(Exception e) {
			return false;
		}
		return true;
	}
	
	private boolean set_single_file(String Book_title, String Genre) {
		try {
			String s = nodeServer.Set(Book_title, Genre);
			if (!s.substring(0,5).equals("ERROR")) {
				System.out.println(s);
				System.out.println("Succeed to set: "+Book_title+" "+Genre);
				return true;
			}else {
				System.out.println("Failed to set: "+Book_title+" "+Genre);
				return false;				
			}
		}catch(Exception e) {
			System.out.println("Failed to set: "+Book_title+" "+Genre);
			return false;
		}
	}
	
	public boolean get(String _s) {
		String Book_title = _s;
		try {			
			String s = nodeServer.Get(Book_title);
			if (!s.substring(0,5).equals("ERROR")) {
				System.out.println("Succeed to get: "+Book_title+".");
				System.out.println(s);
				return true;
			}
			else {
				System.out.println("Failed to find: "+Book_title);
				return false;
			}
			
		}catch(Exception e) {
			System.out.println("Failed to get: "+Book_title);
			return false;
		}		
	}
	
	private boolean connect(TTransport transport, String serviceType) {
		try {			    	    
            transport.open();
    	    TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
    	    if (serviceType.equals("SuperNodeService")){
    	    	superNodeServer = new SuperNodeService.Client(serverProtocol);
    	    }else if (serviceType.equals("NodeService")) {
    	    	nodeServer = new NodeService.Client(serverProtocol);
    	    }        	    
    	    return true;    	    		
		}catch(Exception e) {
			return false;
		}
	}

}
