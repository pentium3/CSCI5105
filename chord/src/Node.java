import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;

import java.net.InetAddress;
import java.util.*;
import java.util.ArrayList;
import java.security.MessageDigest;
import java.util.concurrent.ConcurrentHashMap;

public class Node implements NodeService.Iface {

    public Address NodeInfo;
    public Address SuperInfo;
    public int NumOfNodes;
    public int ChordLen=7;                  //From SuperNode.hash_para
    //public List<FingerItem> FingerTable = new ArrayList<>();    //len=[1..ChordLen]
    public FingerItem[] FingerTable;
    public Address Successor;
    public Address Predecessor;
    public ConcurrentHashMap<String, String>Dict = new ConcurrentHashMap<>();

    public String List2Str(List<Long> L){
        String ans="Trace(";
        for(Long i : L){
            ans=ans+i+", ";
        }
        ans=ans+")";
        return(ans);
    }

    public boolean InSet(long _x, long _i, long _y, int ll, int rr)
    {
        long tx=_x, ti=_i, ty=_y;
        boolean inv=false, ans=true;
        if(tx>ty) {
            inv=true;
            long tmp=tx;
            tx=ty;
            ty=tmp;
        }
        if(ll==0 && rr==0)    ans=(tx<ti && ti<ty);
        if(ll==0 && rr==1)    ans=(tx<ti && ti<=ty);
        if(ll==1 && rr==0)    ans=(tx<=ti && ti<ty);
        if(ll==1 && rr==1)    ans=(tx<=ti && ti<=ty);
        if(inv) return(!ans); else return(ans);
    }

    @Override
    public Address GetSuccessor() {
        //Return successor of current node
        return Successor;
    }

    @Override
    public Address GetPredecessor() {
        //Return predecessor of current node
        return Predecessor;
    }

    @Override
    public void SetPredecessor(Address _a) {
        this.Predecessor = _a.deepCopy();
        System.out.println("My Predecessor is updated to "+this.Predecessor.ip+" "+this.Predecessor.ID);
    }

    @Override
    public void SetSuccessor(Address _a) {
        this.Successor = _a.deepCopy();
        System.out.println("My Successor is updated to "+this.Successor.ip+" "+this.Successor.ID);
    }

    @Override
    public Address FindPredecessor(long MID, List<Long> Visited) throws TException {
        //Find the predecessor of an arbitrary point #MID on chord ring
        Address tmp = NodeInfo.deepCopy();
        long tx=tmp.ID,ti=MID,ty=Successor.ID;
        while(!InSet(tx,ti,ty,0,1)) {
            //if(Visited!=null)
            //    Visited.add(tmp.ID);
            if(tmp.ip.equals(NodeInfo.ip)){
                tmp=FindClosetPrecedingFinger(MID);
            }
            else{
                TTransport transport = new TSocket(tmp.ip, tmp.port);
                transport.open();
                TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client nodeService = new NodeService.Client(serverProtocol);
                tmp=nodeService.FindClosetPrecedingFinger(MID);
                transport.close();
            }
            if(tmp.ip.equals(NodeInfo.ip)){
                ty=Successor.ID;
            }
            else{
                TTransport transport = new TSocket(tmp.ip, tmp.port);
                transport.open();
                TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client nodeService = new NodeService.Client(serverProtocol);
                ty = nodeService.GetSuccessor().ID;
                transport.close();
            }
            tx=tmp.ID;
            ti=MID;
        }
        return(tmp);
    }

    @Override
    public Address FindSuccessor(long MID, List<Long> Visited) throws TException {
        System.out.println("FindSucc caller "+NodeInfo);
        //Find the successor of an arbitrary point #MID on chord ring
        Address tmp=FindPredecessor(MID, Visited);
        if(Visited!=null)
            if(!Visited.contains((tmp.ID)))
                Visited.add(tmp.ID);
        if(tmp.ip.equals(NodeInfo.ip))
            return(Successor);
        TTransport transport = new TSocket(tmp.ip, tmp.port);
        transport.open();
        TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
        NodeService.Client nodeService = new NodeService.Client(serverProtocol);
        Address ans = nodeService.GetSuccessor();
        transport.close();
        System.out.println("FindSucc return "+ans);
        return(ans);
    }

    @Override
    public Address FindClosetPrecedingFinger(long MID) {
        //Find the closet predecessor of an arbitrary point #MID on chord ring, by finding in Finger Table
        for(int i=ChordLen; i>=1; i--) {
            FingerItem FT=FingerTable[i];
            long tx=this.NodeInfo.ID;
            long ti=FT.Node.ID;
            long ty=MID;
            if(InSet(tx,ti,ty,0,0))
                return(FT.Node);
        }
        return this.NodeInfo;
    }

    public void PrintFingerTable() {
        System.out.println("__________________________________________________");
        System.out.println("| Print Finger Table                             |");
        System.out.println("__________________________________________________");
        for(int i=1; i<=ChordLen; i++) {
            System.out.println("|| " + i + " || " + FingerTable[i].Node + " || " + FingerTable[i].start + " ||");
        }
        System.out.println("__________________________________________________");
    }


    public long HashKey(String key, int MODbit) {
        //check whether this node should take charge of this book title
        long MODnum=(long)Math.pow(2, MODbit);
        long lr = 0;
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.update(key.getBytes("UTF8"));
            byte s[] = m.digest();
            String result = "";
            for (int i = 0; i < s.length; i++) {
                lr += (((0x000000FF & s[i]) | 0xFFFFFF00) & 0x000000FF) % MODnum;
                lr=lr % MODnum;
                //result += Integer.toHexString(((0x000000FF & s[i]) | 0xFFFFFF00) & 0x000000FF);
                //System.out.println(Integer.toHexString(((0x000000FF & s[i]) | 0xFFFFFF00) & 0x000000FF));
            }
        }
        catch(Exception e) {
            lr=-1;
            e.printStackTrace();
        }
        return(lr);
    }

    public void InitNode(String SuperIP, int SuperPort, int LocalPort) {
        try {
            NodeInfo = new Address();
            NodeInfo.ip = InetAddress.getLocalHost().getHostName().toString();
            NodeInfo.port = LocalPort;
            SuperInfo = new Address();
            SuperInfo.ip = SuperIP;
            SuperInfo.port = SuperPort;

            TTransport ServerT = new TSocket(SuperInfo.ip, SuperInfo.port);
            ServerT.open();
            TProtocol ServerP = new TBinaryProtocol(new TFramedTransport(ServerT));
            SuperNodeService.Client ServerClient = new SuperNodeService.Client(ServerP);

            Address JoinRet=new Address();
            JoinRet.ip="NACK";
            while(JoinRet.ip.equals("NACK")) {
                JoinRet = ServerClient.Join(NodeInfo.ip, NodeInfo.port);
                Thread.sleep(500);
                System.out.println("NODE INFO: Try joining SuperNode...");
            }
            NodeInfo.ID = ServerClient.GetPossibleKey();
            ChordLen=ServerClient.GetHashPara();
            FingerTable=new FingerItem[ChordLen+1];
            System.out.println("NODE INFO: Successfully joined SuperNode. My ID is "+NodeInfo.ID + ", and ChordLen="+ChordLen);
            if(JoinRet.ip.equals("Empty")) {   //I am the first node in the network.
                for(int i=1;i<=ChordLen;i++)
                    FingerTable[i] = new FingerItem(NodeInfo,
                                                    FingerItem.FingerCalc(NodeInfo.ID, i, ChordLen),
                                                    FingerItem.FingerCalc(NodeInfo.ID, i+1, ChordLen),
                                                    FingerItem.FingerCalc(NodeInfo.ID, i, ChordLen)
                                                    );
                Predecessor=new Address(NodeInfo.ip, NodeInfo.port, NodeInfo.ID);
                Successor=new Address(NodeInfo.ip, NodeInfo.port, NodeInfo.ID);
            }
            else {   //I am assigned to an existing node JoinRet.
                for(int i=1;i<=ChordLen;i++)
                    FingerTable[i] = new FingerItem(null,
                                                    FingerItem.FingerCalc(NodeInfo.ID, i, ChordLen),
                                                    FingerItem.FingerCalc(NodeInfo.ID, i+1, ChordLen),
                                                    FingerItem.FingerCalc(NodeInfo.ID, i, ChordLen)
                                                    );

                System.out.println("Returned JointRes=="+JoinRet.ip+" : "+JoinRet.port);
                TTransport JNodeT = new TSocket(JoinRet.ip, JoinRet.port);
                JNodeT.open();
                TProtocol JNodeP = new TBinaryProtocol(new TFramedTransport(JNodeT));
                NodeService.Client JNodeClient = new NodeService.Client(JNodeP);
                Address JointResSucc=JNodeClient.GetSuccessor();

                TTransport JSNodeT = new TSocket(JointResSucc.ip, JointResSucc.port);
                JSNodeT.open();
                TProtocol JSNodeP = new TBinaryProtocol(new TFramedTransport(JSNodeT));
                NodeService.Client JSNodeClient = new NodeService.Client(JSNodeP);

                Successor=JointResSucc.deepCopy();
                Predecessor=JoinRet.deepCopy();
                JNodeClient.SetSuccessor(NodeInfo);
                JSNodeClient.SetPredecessor(NodeInfo);
                System.out.println("My Successor is "+Successor+" . My Predecessor is "+Predecessor);

                FingerTable[1].Node=Successor;
                for(int i=1;i<=ChordLen-1;i++){
                    long tx=NodeInfo.ID;
                    long ty=FingerTable[i].Node.ID;
                    long ti=FingerTable[i+1].start;
                    if(InSet(tx,ti,ty,1,0)) {
                        FingerTable[i+1].Node = FingerTable[i].Node.deepCopy();
		            }
                    else {
                        FingerTable[i+1].Node = JNodeClient.FindSuccessor(FingerTable[i+1].start, null);
		            }
                }

                for(int i=1; i<=ChordLen; i++){
                    long pid=NodeInfo.ID-(long)Math.pow(2,i-1);
                    while(pid<0)
                        pid+=Math.pow(2,ChordLen);
                    Address p=FindPredecessor(pid, null);
                    if(p.ip.equals(NodeInfo.ip))
                        UpdateDHT(NodeInfo, i, new ArrayList<Long>(Arrays.asList(NodeInfo.ID)));
                    else{
                        TTransport PNodeT = new TSocket(p.ip, p.port);
                        PNodeT.open();
                        TProtocol PNodeP = new TBinaryProtocol(new TFramedTransport(PNodeT));
                        NodeService.Client PNodeClient = new NodeService.Client(PNodeP);
                        PNodeClient.UpdateDHT(NodeInfo, i, new ArrayList<Long>(Arrays.asList(NodeInfo.ID)));
                        PNodeT.close();
                    }
                }

                JNodeT.close();
                JSNodeT.close();
            }

            ServerClient.PostJoin(NodeInfo.ip, NodeInfo.port);
            ServerT.close();
            PrintFingerTable();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void UpdateDHT(Address NN, int IDX, List<Long> chain) throws TException {
        try{
            chain.add(NodeInfo.ID);
            //node call this to update this.FingerTable
            long tx=NodeInfo.ID;
            long ty=FingerTable[IDX].Node.ID;
            long ti=NN.ID;
	        if(InSet(ty,FingerTable[IDX].start,ti,1,1)) {
                //System.out.println("UpdateDHT Item "+NN+" : "+IDX);
		        FingerTable[IDX].Node=NN.deepCopy();
                Address p = Predecessor.deepCopy();
                if(!chain.contains(p.ID)){
			        //System.out.println("UpdateDHT Forward to "+p);
                    TTransport PNodeT = new TSocket(p.ip, p.port);
                    PNodeT.open();
                    TProtocol PNodeP = new TBinaryProtocol(new TFramedTransport(PNodeT));
                    NodeService.Client PNodeClient = new NodeService.Client(PNodeP);
                    PNodeClient.UpdateDHT(NN, IDX, chain);
                    PNodeT.close();
                }
            }
            PrintFingerTable();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public boolean ping() throws TException   {
        return(true);
    }

    @Override
    public String Set(String BookTitle, String Genre) {
        long HSK = HashKey(BookTitle, ChordLen);
        try {
            System.out.println("SET INFO: trying to set "+ BookTitle);
            //client call this to add element in DHT
            if(InSet(Predecessor.ID, HSK, NodeInfo.ID, 0, 1)) {
                Dict.put(BookTitle, Genre);
                System.out.println("SET INFO: " + BookTitle + " is set on this machine");
                return ("["+BookTitle + " is set on machine "+NodeInfo.ID+" with hash value= "+HSK+"]");      //1: successfully added on local node
            } else {
                //Search for node to forward on Finger Table, and call Set() on this node
                List<Long> Visited = new ArrayList<Long>();
                Visited.add(NodeInfo.ID);
                Address ForwardMachine = FindSuccessor(HSK, Visited);
                if(ForwardMachine==null)
                    throw new Exception("SET ERROR: Didn't find machine in FingerTable");
                System.out.println("SET INFO: Forwarding to Node "+ForwardMachine.ID);
                Visited.add(ForwardMachine.ID);
                String Trace=List2Str(Visited);
                TTransport transport = new TSocket(ForwardMachine.ip, ForwardMachine.port);
                transport.open();
                TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client nodeService = new NodeService.Client(serverProtocol);
                String ret = nodeService.Set(BookTitle, Genre);
                transport.close();
                return(ret+Trace);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
            return("SET ERROR: FAILED setting");     //-1: failed to add
        }
    }

    @Override
    public String Get(String BookTitle) {
        long HSK = HashKey(BookTitle, ChordLen);
        String ans = null;
        try {
            System.out.println("GET INFO: trying to get "+ BookTitle);
            //client call this to get element in DHT
            if(InSet(Predecessor.ID, HSK, NodeInfo.ID, 0, 1)) {
                ans = Dict.get(BookTitle);
                if(ans!=null){
                    System.out.println("GET INFO: " + BookTitle + " is got on this machine");
                    return ("["+BookTitle+":"+ans + " is get on machine "+NodeInfo.ID+" with hash value= "+HSK+"]");
                }
                else{
                    System.out.println("GET ERROR: " + BookTitle + " should be on this machine, but I can not find it");
                    return ("ERROR["+BookTitle + " is NOT FOUND on machine "+NodeInfo.ID+" with hash value= "+HSK+"]");
                }
            }
            else {
                //Search for node to forward on Finger Table
                List<Long> Visited = new ArrayList<Long>();
                Visited.add(NodeInfo.ID);
                Address ForwardMachine = FindSuccessor(HSK, Visited);
                if(ForwardMachine==null)
                    throw new Exception("GET ERROR: Didn't find machine in FingerTable");
                System.out.println("GET INFO: Forwarding to Node "+ForwardMachine.ID);
                Visited.add(ForwardMachine.ID);
                String Trace=List2Str(Visited);
                TTransport transport = new TSocket(ForwardMachine.ip, ForwardMachine.port);
                transport.open();
                TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client nodeService = new NodeService.Client(serverProtocol);
                ans = nodeService.Get(BookTitle);
                transport.close();
                return (ans+Trace);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return("GET ERROR: FAILED getting");     //-1: failed to add
        }
    }

    public void start() throws TTransportException {
        TServerTransport serverTransport = new TServerSocket(NodeInfo.port);
        TTransportFactory factory = new TFramedTransport.Factory();
        NodeService.Processor processor = new NodeService.Processor<>(this);

        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(factory);

        TServer server = new TThreadPoolServer(serverArgs);
        server.serve();
    }

    public static void main(String[] args) {
        //initialize this node
        //call Join() to register this node on SuperNode
        //and update node info in this node
        if(args.length<3) {
            System.out.println("Usage: java Node <ServerIP> <ServerPort> <NodePort>");
            return;
        }
        try {
            System.out.println("IP Address of this node: "+InetAddress.getLocalHost().toString());
            String ServerIP = args[0];
            Integer ServerPort = Integer.parseInt(args[1]);
            Integer LocalPort = Integer.parseInt(args[2]);
            Node NodeItem = new Node();
            NodeItem.InitNode(ServerIP, ServerPort, LocalPort);
            NodeItem.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
