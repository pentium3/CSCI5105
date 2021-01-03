import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap; 
import java.lang.Math;

public class SuperNode implements SuperNodeService.Iface {
	CopyOnWriteArrayList<Address> currentNodes = new CopyOnWriteArrayList<>();
	CopyOnWriteArrayList<Integer> possibleKeys = new CopyOnWriteArrayList<>();
	ConcurrentHashMap<Address,Integer> nodeKeys = new ConcurrentHashMap<>();
	
	int n_nodes;
	int n_keys;
	boolean waitJoin = false;
	Random random;
    public int hash_para;

    @Override
    public int GetHashPara()
    {
        return(this.hash_para);
    }
    
    public static void main(String [] args) {
        if(args.length<2)
        {
            System.out.println("Usage: java SuperNode <ServerPort> <Number of Node>");
            return;
        }
        try {
        SuperNode superNode = new SuperNode();
        superNode.initialize(args);       
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    private void initialize(String[] args) {
        try {
	random = new Random();
        Integer ServerPort = Integer.parseInt(args[0]);
        this.n_nodes = Integer.parseInt(args[1]);
        if (args.length>2) {
        	hash_para = Integer.parseInt(args[2]);
        }else {
        	hash_para = 7;
        }
        this.n_keys = (int)Math.pow(2, hash_para);
        int step = (int)Math.floor(n_keys/n_nodes);
        for(int i=0;i<n_nodes;i++){
           possibleKeys.add(Integer.valueOf(i*step));
        }   
        
        TServerTransport serverTransport = new TServerSocket(ServerPort);
        TTransportFactory factory = new TFramedTransport.Factory();
        SuperNodeService.Processor processor = new SuperNodeService.Processor<>(this);
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor); 
        serverArgs.transportFactory(factory); 
        TServer server = new TThreadPoolServer(serverArgs);
        server.serve();
        }catch(TException e) {
        	e.printStackTrace();
        }catch(Exception e) {
            e.printStackTrace();
        }    	
    }

    @Override
    public boolean ping() throws TException
    {
        return(true);
    }

    @Override
    public synchronized Address Join(String IP, int port)
    {try {	
	if (this.currentNodes.size()<this.n_nodes && !this.waitJoin) {
    		this.waitJoin = true;
    		if (this.currentNodes.size()==0) {
    			return new Address("Empty", -1, -1);   //if n is the only node in the network, return "EMPTY"
    		}                                                       //  else, return a random node
		return this.currentNodes.get(this.currentNodes.size()-1);
    	}else {
    		System.out.println("Already have "+this.currentNodes.size()+" nodes.");
    		Address NACK_ADDRESS = new Address("NACK", -1, -1);
    		return NACK_ADDRESS;
    	}
    }catch(Exception e) {
    	e.printStackTrace();
		System.out.println("Already have "+this.currentNodes.size()+" nodes.");
		Address NACK_ADDRESS = new Address("No", -1, -1);
		return NACK_ADDRESS;    	
    }
    }
    
    
    @Override
    public int GetPossibleKey()
    {
    	if (this.currentNodes.size()<n_nodes) {
    		return this.possibleKeys.get(this.currentNodes.size());
    	}else {
    		System.out.println("Already have "+n_nodes+" nodes.");
    		return -1;
    	}
    }
   

    @Override
    public synchronized boolean PostJoin(String IP, int port)
    {	try {
    	if (!this.waitJoin) {
    		System.out.println("Failed to postjoin "+IP+":"+port);
    		return false;
    	}
		int key = this.possibleKeys.get(this.currentNodes.size());
		Address newNode = new Address(IP, port, key);
		this.currentNodes.add(newNode);
		this.nodeKeys.put(newNode, key);
		this.waitJoin = false;
		System.out.println("Postjoin "+IP+":"+port);
		return true;
    }catch(Exception e) {
    	e.printStackTrace();
    	System.out.println("Failed to postjoin "+IP+":"+port);
    	return false;
    }
    }

    @Override
    public Address GetNode()
    {
    	if (this.currentNodes.size()<this.n_nodes) {
    		System.out.println("Only have "+n_nodes+" nodes.");
    		return new Address("NACK", -1, -1);
    	}
    	return this.currentNodes.get(this.random.nextInt(this.currentNodes.size()));
    }
}
