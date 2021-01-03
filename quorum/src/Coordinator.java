import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;
import java.net.*; 
import java.io.*; 
import java.util.*; 
import java.net.InetAddress; 

import java.io.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

public class Coordinator extends Node implements CoordinatorService.Iface {
	ConcurrentHashMap<String, String> FSLock;
	ConcurrentHashMap<String, Integer> FSVersion;
	ConcurrentHashMap<String, Integer> FSCurrentReadNumber;
	ConcurrentLinkedDeque<Request> requestQueue; 
	ArrayList<Address> serverList;
	NodeService.Client nodeServer;
	TServer server;
	int NR;
	int NW;
	Address selfaddress;
	
	public Coordinator(String[] args) {
        try {
	        Integer ServerPort = Integer.parseInt(args[0]);
	        this.NR = Integer.parseInt(args[1]);
	        this.NW = Integer.parseInt(args[2]);
	        this.FSLock = new ConcurrentHashMap<>();
	        this.FSVersion = new ConcurrentHashMap<>();
	        this.FSCurrentReadNumber = new ConcurrentHashMap<>();
	        this.requestQueue = new ConcurrentLinkedDeque<>();
	        this.serverList = new ArrayList<Address>();
	        
	        TServerTransport serverTransport = new TServerSocket(ServerPort);
	        TTransportFactory factory = new TFramedTransport.Factory();
	        CoordinatorService.Processor processor = new CoordinatorService.Processor<>(this);
	        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
	        serverArgs.processor(processor); 
	        serverArgs.transportFactory(factory); 
	        this.server = new TThreadPoolServer(serverArgs);
		this.selfaddress = new Address(InetAddress.getLocalHost().getHostAddress(),ServerPort);
		this.serverList.add(this.selfaddress);
	        System.out.println("Coordinator is listening on "+InetAddress.getLocalHost().getHostAddress()+":"+ServerPort);
        }catch(TException e) {
        	e.printStackTrace();
        }catch(Exception e) {
            e.printStackTrace();
        } 
	}

	public static void main(String[] args) {
        if(args.length<3)
        {
            System.out.println("Usage: java Coordinator <ServerPort> <NR> <NW>");
            return;
        }
        try {
        	Coordinator coordinatorService = new Coordinator(args); 
        	coordinatorService.cord_start();
        }catch(Exception e) {
            e.printStackTrace();
        }

	}

	@Override
	public String Read(String FileName){
		//client call Read() to read a file
		String ans="";
		try {
			ans = this.Coord_Read(FileName);
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return(ans);
	}

	@Override
	public int Write(String FileName, String FileContent){
		//client call Write() to write a file
		int ret=-1;
		try{
			boolean writeres=this.Coord_Write(FileName, FileContent);
			if(writeres)
				ret=1;
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return (ret);
	}

	@Override
	public Map<String, Integer> lsDir(){
		//client call Write() to write a file
		Map<String, Integer> ret=new HashMap<>();
		try{
			ret=this.Coord_lsDir();
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return (ret);
	}

	@Override
	public boolean Coord_reset(int nr, int nw){
		boolean ans=false;
		try {
			ans = this.reset(nr,nw);
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return(ans);
	}


	public String Coord_Read(String filename) {
		try {
			if (!(this.NR+this.NW>this.serverList.size() && 2*this.NW>this.serverList.size())) {
				return String.format("NR = %d, NW = %d, N = %d, please reset <NR> <NW>", 
						this.NR, this.NW, this.serverList.size());
			}
			if (!this.FSLock.containsKey(filename)) {
				return "Can't find "+filename;
			}
			Request request = new Request(filename);
			this.requestQueue.add(request);
			while (this.requestQueue.size()==0 || !this.requestQueue.peek().equals(request));
				//System.out.println("wait request");
			this.requestQueue.poll();
			while (FSLock.get(filename).equals("write"));
				//System.out.println("wait file unlock");
			this.FSCurrentReadNumber.put(filename, this.FSCurrentReadNumber.getOrDefault(filename, 0)+1);
			FSLock.put(filename, "read");	
			List<Address> quorum = this.getQuorum(this.NR);
			int latestVersionNumber = -1;
			Address latestServer = new Address("None", 0);
			for (Address server: quorum) {
				if (server.ip.equals(this.selfaddress.ip)) {
			    	if (this.GetVersion(filename) > latestVersionNumber) {
			    		latestVersionNumber = this.GetVersion(filename);
			    		latestServer = server;
			    		
			    	}	
				} else {
			    	TTransport transport = new TSocket(server.ip, server.port);
			    	NodeService.Client nodeServer = connect(transport);
			    	if (nodeServer.GetVersion(filename) > latestVersionNumber) {
			    		latestVersionNumber = nodeServer.GetVersion(filename);
			    		latestServer = server;
			    		
			    	}
			    	transport.close();			
				}
			}
			if (this.FSCurrentReadNumber.getOrDefault(filename, 0)<=0) this.FSCurrentReadNumber.put(filename, 0);
			else this.FSCurrentReadNumber.put(filename, this.FSCurrentReadNumber.getOrDefault(filename, 0)-1);
			if (latestVersionNumber == -1) {
				if (this.FSCurrentReadNumber.getOrDefault(filename, 0)==0) {
					FSLock.put(filename, "free");
				}
				return "Can't find "+filename;
			}else {
				if (this.FSCurrentReadNumber.getOrDefault(filename, 0)==0) FSLock.put(filename, "free");
		    	if (latestServer.ip.equals(this.selfaddress.ip)) return this.DirectRead(filename);
				TTransport transport = new TSocket(latestServer.ip, latestServer.port);
				NodeService.Client nodeServer = connect(transport);
		    	
		    	String filecontent = nodeServer.DirectRead(filename);
		    	transport.close();	
		    	return filecontent;
			}
		}catch(Exception e) {
			if (this.FSCurrentReadNumber.getOrDefault(filename, 0)>0) this.FSCurrentReadNumber.put(filename, 0);
			e.printStackTrace();
			System.out.println("Failed to read "+filename);
			return "Can't find "+filename;
		}
			
	}
	
	public boolean Coord_Write(String filename, String fileContent) {
		try {
			if (!(this.NR+this.NW>this.serverList.size() && 2*this.NW>this.serverList.size())) {
				System.out.println(String.format("NR = %d, NW = %d, N = %d, please reset <NR> <NW>", 
						this.NR, this.NW, this.serverList.size()));
				return false;
			}
			Request request = new Request(filename, fileContent);
			this.requestQueue.add(request);
			while (this.requestQueue.size()==0||!this.requestQueue.peek().equals(request));
				//System.out.println("wait request");
			this.requestQueue.poll();	
			
			
			while (FSLock.get(filename)!=null && !FSLock.get(filename).equals("free"));
                //System.out.println("wait file unlock to write"+filename+FSLock.get(filename)+this.FSCurrentReadNumber.getOrDefault(filename, 0));

			FSLock.put(filename, "write");	
			List<Address> quorum = this.getQuorum(this.NW);
			int latestVersionNumber = -1;
			Address latestServer;
			for (Address server: quorum) {
		    	if (server.ip.equals(this.selfaddress.ip)) {
		    		latestVersionNumber = this.GetVersion(filename);
		    		latestServer = server;		    		
		    	}else {
					TTransport transport = new TSocket(server.ip, server.port);
					NodeService.Client nodeServer = this.connect(transport);
			    	if (nodeServer.GetVersion(filename)> latestVersionNumber) {
			    		latestVersionNumber = nodeServer.GetVersion(filename);
			    		latestServer = server;
			    	}
			    	transport.close();			
		    	}
			}
			latestVersionNumber += 1;
			for (Address server: quorum) {
		    	if (server.ip.equals(this.selfaddress.ip)) {
		    		this.DirectWrite(filename, fileContent, latestVersionNumber);
		    	}else {
					TTransport transport = new TSocket(server.ip, server.port);
					NodeService.Client nodeServer = connect(transport);
			    	nodeServer.DirectWrite(filename, fileContent, latestVersionNumber);
			    	transport.close();	
		    	}
			}

			this.FSVersion.put(filename, latestVersionNumber);
			this.FSLock.put(filename, "free");
			System.out.println("Succeed to write "+filename);
			return true;
		}catch(Exception e) {
			System.out.println("Failed to write "+filename);
			e.printStackTrace();
			return false;
		}
		
	}
	
	public boolean Join(Address server) {
		try {
			this.serverList.add(server);
			System.out.println(String.format("NR = %d, NW = %d, N = %d", 
					this.NR, this.NW, this.serverList.size()));
			System.out.println("Succeed to join "+server.ip+":"+server.port);
			return true;
		}catch(Exception e) {
			System.out.println("Failed to join "+server.ip+":"+server.port);
			return false;			
		}
	}
	
	public boolean reset(int NR, int NW) {
		if (!(NR+NW>this.serverList.size() && 2*NW>this.serverList.size())) {
			System.out.println(String.format("NR = %d, NW = %d, N = %d, please reset <NR> <NW>", 
					NR, NW, this.serverList.size()));
			return false;
		}		
		this.NR = NR;
		this.NW = NW;
		System.out.println(String.format("Reset NR = %d, NW = %d, N = %d", 
					this.NR, this.NW, this.serverList.size()));
		return true;
		
	}
	
    public List<Address> getQuorum(int totalItems) 
	{ 
		Random rand = new Random(); 
		List<Address> newList = new ArrayList<>(); 
		List<Integer> list = IntStream.range(0, this.serverList.size()).boxed().collect(Collectors.toList());
		for (int i = 0; i < totalItems; i++){
		int randomIndex = rand.nextInt(list.size());  
			newList.add(this.serverList.get(list.get(randomIndex))); 
			list.remove(randomIndex); 
		} 
		return newList; 
	} 
	
	private NodeService.Client connect(TTransport transport) {
		try {			    	    
            transport.open();
    	    TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
	    	return new NodeService.Client(serverProtocol);    	    		
		}catch(Exception e) {
			return null;
		}
	}
	
	public void sync(){
		while(true) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			for (String filename: this.FSLock.keySet()){
				try {
					//if (this.requestQueue.size()==0){
						if (FSLock.get(filename)!=null && this.FSLock.get(filename).equals("free")) {
							this.FSLock.put(filename, "write");

							int latestVersionNumber = -1;
							boolean need_to_update = false;
							Address latestServer = new Address("None", 0);
							for (Address server: this.serverList) {
						    	if (server.ip.equals(this.selfaddress.ip)) {
							    	if (this.GetVersion(filename)> latestVersionNumber) {
							    		latestVersionNumber = this.GetVersion(filename);
							    		latestServer = server;
							    	}	
							    	if (this.GetVersion(filename)<this.FSVersion.getOrDefault(filename, -1))
							    		need_to_update = true;
						    	}else {
									TTransport transport = new TSocket(server.ip, server.port);
									NodeService.Client nodeServer = this.connect(transport);
							    	if (nodeServer.GetVersion(filename)> latestVersionNumber) {
							    		latestVersionNumber = nodeServer.GetVersion(filename);
							    		latestServer = server;
							    	}
							    	if (nodeServer.GetVersion(filename)<this.FSVersion.getOrDefault(filename, -1))
							    		need_to_update = true;
							    	transport.close();
						    	}
						    	
							}
							if (need_to_update) {
								System.out.println("Synchronize "+filename);
								String fileContent = "";
						    	if (latestServer.ip.equals(this.selfaddress.ip)) {
						    		fileContent = this.DirectRead(filename);			    		
						    	}else {
									TTransport transport = new TSocket(latestServer.ip, latestServer.port);
									NodeService.Client nodeServer = this.connect(transport);
							    	fileContent = nodeServer.DirectRead(filename);
							    	transport.close();	
						    	}							
								for (Address server: this.serverList){
							    	if (server.ip.equals(this.selfaddress.ip)) {
								    	this.DirectWrite(filename, fileContent, latestVersionNumber);			    		
							    	}else {
										TTransport transport = new TSocket(server.ip, server.port);
										NodeService.Client nodeServer = this.connect(transport);
								    	nodeServer.DirectWrite(filename, fileContent, latestVersionNumber);
								    	transport.close();	
							    	}
								}
								System.out.println(filename+ " synchronized to version "+latestVersionNumber);
							}
							
							this.FSLock.put(filename, "free");
							
						}
					//}
				}catch(Exception e) {
					this.FSLock.put(filename, "free");
					e.printStackTrace();
					System.out.println("Failed to synchronize "+filename);
				}
			}
		}
	}
	
	public void cord_start() {
	   class MyRunnable implements Runnable {
	      private Coordinator coordinatorService;
	      public MyRunnable(Coordinator coordinatorService) {
	         this.coordinatorService = coordinatorService;
	      }

	      public void run() {
	    	  this.coordinatorService.sync();
	      }
	   }
	   Thread t = new Thread(new MyRunnable(this));
	   t.start();
	   System.out.println("Background synchronization is running.");

	   this.server.serve();
	}
	
	public ConcurrentHashMap<String, Integer> Coord_lsDir() {
		return this.FSVersion;
	}
	
	

}
