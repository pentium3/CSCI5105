
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;

import java.io.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
*/

public class Node implements NodeService.Iface {
    ConcurrentHashMap<String, Integer> FSVer=new ConcurrentHashMap<>();
    String WorkDir="./ServerDir_"+System.currentTimeMillis()%100000000+"/";
    Address NodeInfo;
    Address CoordinatorInfo;

    @Override
    public String Read(String FileName){
        //client call Read() to read a file
        String ans="";
        try {
            TTransport ServerT = new TSocket(CoordinatorInfo.ip, CoordinatorInfo.port);
            ServerT.open();
            TProtocol ServerP = new TBinaryProtocol(new TFramedTransport(ServerT));
            CoordinatorService.Client CoordClient = new CoordinatorService.Client(ServerP);

            ans = CoordClient.Coord_Read(FileName);

            ServerT.close();
        }
        catch (TException e){
            e.printStackTrace();
        }
        return(ans);
    }

    @Override
    public int Write(String FileName, String FileContent){
        //client call Write() to write a file
        int ret=-1;
        try{
            TTransport ServerT = new TSocket(CoordinatorInfo.ip, CoordinatorInfo.port);
            ServerT.open();
            TProtocol ServerP = new TBinaryProtocol(new TFramedTransport(ServerT));
            CoordinatorService.Client CoordClient = new CoordinatorService.Client(ServerP);

            boolean writeres=CoordClient.Coord_Write(FileName, FileContent);
            if(writeres)
                ret=1;

            ServerT.close();
        }
        catch (TException e){
            e.printStackTrace();
        }
        return (ret);
    }

    @Override
    public Map<String, Integer> lsDir(){
        //client call Write() to write a file
        Map<String, Integer> ret=new HashMap<>();
        try{
            TTransport ServerT = new TSocket(CoordinatorInfo.ip, CoordinatorInfo.port);
            ServerT.open();
            TProtocol ServerP = new TBinaryProtocol(new TFramedTransport(ServerT));
            CoordinatorService.Client CoordClient = new CoordinatorService.Client(ServerP);

            ret=CoordClient.Coord_lsDir();
            //TODO: need .deepCopy() ???

            ServerT.close();
        }
        catch (TException e){
            e.printStackTrace();
        }
        return (ret);
    }

    @Override
    public int GetVersion(String FileName){
        //coordinator call this to get the version of file on this server
        Integer ans=FSVer.get(FileName);
        if(ans==null){
            //System.out.println("GetVersion of file "+FileName+" : -1");
            return(-1);
        }
        else{
            //System.out.println("GetVersion of file "+FileName+" : "+ans.intValue());
            return ans.intValue();
        }
    }

    @Override
    public boolean Coord_reset(int nr, int nw){
        boolean ans=false;
        try {
            TTransport ServerT = new TSocket(CoordinatorInfo.ip, CoordinatorInfo.port);
            ServerT.open();
            TProtocol ServerP = new TBinaryProtocol(new TFramedTransport(ServerT));
            CoordinatorService.Client CoordClient = new CoordinatorService.Client(ServerP);

            ans = CoordClient.reset(nr,nw);

            ServerT.close();
        }
        catch (TException e){
            e.printStackTrace();
        }
        return(ans);
    }

    @Override
    public String DirectRead(String FileName){
        //a coordinator call DirectRead() to read file
        String FileContent="";
        try{
            String FilePath=WorkDir+FileName;
            System.out.println("DirectRead: "+FilePath);
            FileReader reader = new FileReader(FilePath);
            BufferedReader br = new BufferedReader(reader);
            String line;
            while ((line = br.readLine()) != null) {
                FileContent+=line;
            }
        } catch (FileNotFoundException ef) {
            System.out.println("Server ERROR: File "+FileName+" not found. ");
            return("NULL");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return (FileContent);
    }

    @Override
    public int DirectWrite(String FileName, String FileContent, int FileNewVer){
        //a coordinator call DirectWrite() to write file
        int ret=1;
        try {
            String FilePath=WorkDir+FileName;
            System.out.println("DirectWrite: "+FilePath+" with version=="+FileNewVer);
            File fHandler = new File(FilePath);
            File fileParent = fHandler.getParentFile();
            if(!fileParent.exists()){
                fileParent.mkdirs();
            }
            fHandler.createNewFile();
            FileWriter writer = new FileWriter(fHandler);
            BufferedWriter out = new BufferedWriter(writer);
            out.write(FileContent);
            out.flush();
            FSVer.put(FileName, FileNewVer);
        } catch (IOException e) {
            e.printStackTrace();
            ret=-1;
        }
        return(ret);
    }

    public void InitServer(String CoordIP, Integer CoordPort, String LocalIP, Integer LocalPort){
        try {
            System.out.println("My working dir is " + WorkDir);

            NodeInfo = new Address(LocalIP, LocalPort);
            CoordinatorInfo = new Address(CoordIP, CoordPort);

            TTransport ServerT = new TSocket(CoordinatorInfo.ip, CoordinatorInfo.port);
            ServerT.open();
            TProtocol ServerP = new TBinaryProtocol(new TFramedTransport(ServerT));
            CoordinatorService.Client CoordClient = new CoordinatorService.Client(ServerP);

            CoordClient.Join(NodeInfo);

            ServerT.close();
        }
        catch (TException e){
            e.printStackTrace();
        }
    }

    public void start() throws TTransportException{
        TServerTransport serverTransport = new TServerSocket(NodeInfo.port);
        TTransportFactory factory=new TFramedTransport.Factory();
        NodeService.Processor processor=new NodeService.Processor<>(this);

        TThreadPoolServer.Args serverArgs=new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(factory);

        TServer server=new TThreadPoolServer(serverArgs);
        server.serve();
    }

    public static void main(String[] args) {
        if(args.length<3){
            System.out.println("Usage: java Node <CoordinatorIP> <CoordinatorPort> <NodePort>");
        }
        try {
            String CIP=args[0];
            Integer CPort=Integer.parseInt(args[1]);
            String LocalIP=InetAddress.getLocalHost().getHostAddress();
            Integer LocalPort=Integer.parseInt(args[2]);
            System.out.println("IP Address of this server: "+ LocalIP);
            Node ServerItem = new Node();
            ServerItem.InitServer(CIP, CPort, LocalIP, LocalPort);
            ServerItem.start();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
