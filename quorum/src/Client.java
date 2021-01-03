

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
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

public class Client {
    Address ServerNode = new Address();
    NodeService.Client server;
    TTransport serverTransport;
    Scanner sc;
    String WorkDir="./ClientDir/";

    public static void main(String [] args) {
        if(args.length < 2) {
            System.err.println("Usage: java Client nodeIP nodePort");
            //user could indicate an arbitrary server
            return;
        }
        try {
            Client client = new Client();
            System.out.println("My working dir is "+client.WorkDir);
            int cans=client.initialize(args);
            if(cans==-1)
                return;
            while(true){
                int res=client.UserInterface();
                if(res==-1)
                    break;
            }
            client.serverTransport.close();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    private String ReadTXT(String lfile) throws IOException {
        FileReader reader = new FileReader(lfile);
        BufferedReader br = new BufferedReader(reader);
        String line, content="";
        while ((line = br.readLine()) != null) {
            content+=line;
        }
        return (content);
    }

    private int initialize(String [] args) throws UnknownHostException {
        try{
            ServerNode.ip = args[0];
            ServerNode.port = Integer.parseInt(args[1]);
            serverTransport = new TSocket(ServerNode.ip, ServerNode.port);
            serverTransport.open();
            TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
            server = new NodeService.Client(serverProtocol);

            File _f=new File(WorkDir);
            if(!_f.exists())
                _f.mkdirs();
            return(1);
        }
        catch (Exception e){
            e.printStackTrace();
            return(-1);
        }
    }

    private int UserInterface(){
        try {
            System.out.println("*************************************************************************************");
            System.out.println("Operations:");
            System.out.println("    <setdir> local_dirname : Set the local working directory");
            System.out.println("    <getdir> : Show the local working directory");
            System.out.println("    <read> remote_filename : Read a remote file");
            System.out.println("    <write> remote_filename local_filename : Write a remote file with the content of a local file");
            System.out.println("    <lsremote> : Show the list of all remote files");
            System.out.println("    <lslocal> : Show the list of all local files");
            System.out.println("    <bench-write> : Perform a benchmark: write all files in local working directory to remote file system");
            System.out.println("    <bench-read> : Perform a benchmark: read all files in remote file system");
            System.out.println("    <benchmark> tr tw : Perform a benchmark: tw times of [bench-write], then tr times of [bench-read]");
            System.out.println("    <setcod> nr nw : set NR / NW on coordinator");
            System.out.println("    <quit> : Quit");
            System.out.println("*************************************************************************************");
            sc = new Scanner(System.in);
            String[] cmd = sc.nextLine().split(" ");
            System.out.println(cmd[0]);
            long StartTime=System.currentTimeMillis();
            if(cmd[0].equals("setdir")){
                if(cmd.length<2){
                    System.out.println("Invalid command");
                    return(0);
                }
                String ldir=cmd[1].trim();
                File _f=new File(ldir);
                if(!_f.isDirectory()){
                    System.out.println("setdir ERROR: Invalid directory");
                    return(0);
                }
                WorkDir=ldir;
                System.out.println("setdir INFO: WorkDir is set to "+WorkDir);
            }
            else if(cmd[0].equals("getdir")){
                System.out.println("getdir INFO: "+WorkDir);
            }
            else if(cmd[0].equals("read")){
                if(cmd.length<2){
                    System.out.println("Invalid command");
                    return(0);
                }
                String rfile=cmd[1].trim();
                String ans = server.Read(rfile);
                System.out.println("read INFO: FileName = "+rfile);
                System.out.println("read INFO: Content  = "+ans);
            }
            else if(cmd[0].equals("write")){
                if(cmd.length<3){
                    System.out.println("Invalid command");
                    return(0);
                }
                String rfile=cmd[1].trim();
                String lfile=WorkDir+cmd[2].trim();
                File _f=new File(lfile);
                if(!_f.isFile()){
                    System.out.println("write ERROR: Invalid local file name");
                    return(0);
                }
                String content=ReadTXT(lfile);
                System.out.println("write INFO: local file content: "+content);
                int ret= server.Write(rfile, content);
                System.out.println("write INFO: write return value = "+ret);
            }
            else if(cmd[0].equals("lsremote")){
                Map<String, Integer> ans= server.lsDir();
                for(String e: ans.keySet()){
                    System.out.println("lsremote INFO: "+e+" || "+ans.get(e));
                }
            }
            else if(cmd[0].equals("lslocal")){
                File file=new File(WorkDir);
                for(File temp:file.listFiles()){
                    if(temp.isFile()){
                        System.out.println("lslocal INFO: "+temp.toString());
                    }
                }
            }
            else if(cmd[0].equals("bench-write")){
                // write all files in local working directory to remote file system
                File file=new File(WorkDir);
                for(File temp:file.listFiles()){
                    if(temp.isFile()){
                        String fpath=temp.toString();
                        String content=ReadTXT(fpath);
                        String fname=fpath.replace(WorkDir,"");
                        int ret= server.Write(fname, content);
                        System.out.println("bench-write INFO: write filename = "+fname+" return value = "+ret);
                        System.out.println("bench-write INFO: ----------------------");
                    }
                }
            }
            else if(cmd[0].equals("bench-read")){
                // read all files in remote file system
                Map<String, Integer> ans= server.lsDir();
                for(String e: ans.keySet()){
                    System.out.println("bench-read INFO: "+e+" || "+ans.get(e));
                    String content = server.Read(e);
                    System.out.println("bench-read INFO: "+content);
                    System.out.println("bench-read INFO: ----------------------");
                }
            }
            else if(cmd[0].equals("benchmark")){
                if(cmd.length<3){
                    System.out.println("Invalid command");
                    return(0);
                }
                String _nr=cmd[1].trim();
                String _nw=cmd[2].trim();
                Integer nr=Integer.valueOf(_nr);
                Integer nw=Integer.valueOf(_nw);
                long t1=System.currentTimeMillis();
                for(int i=0;i<nw;i++){
                    File file=new File(WorkDir);
                    for(File temp:file.listFiles()){
                        if(temp.isFile()){
                            String fpath=temp.toString();
                            String content=ReadTXT(fpath);
                            String fname=fpath.replace(WorkDir,"");
                            int ret= server.Write(fname, content);
                            System.out.println("benchmark INFO: write filename = "+fname+" return value = "+ret);
                        }
                    }
                }
                long t2=System.currentTimeMillis();
                for(int i=0;i<nr;i++){
                    Map<String, Integer> ans= server.lsDir();
                    for(String e: ans.keySet()){
                        String content = server.Read(e);
                        System.out.println("benchmark INFO: read filename = "+e+" || version = "+ans.get(e)+" || content = "+content);
                    }
                }
                long t3=System.currentTimeMillis();
                long d1=t2-t1;
                long d2=t3-t2;
                System.out.println("benchmark INFO: write time "+d1);
                System.out.println("benchmark INFO: read time "+d2);
            }
            else if(cmd[0].equals("setcod")){
                if(cmd.length<3){
                    System.out.println("Invalid command");
                    return(0);
                }
                String _nr=cmd[1].trim();
                String _nw=cmd[2].trim();
                Integer nr=Integer.valueOf(_nr);
                Integer nw=Integer.valueOf(_nw);
                boolean res=server.Coord_reset(nr,nw);
                if(res)
                    System.out.println("setcod INFO: set successfully");
                else
                    System.out.println("setcod ERROR: set failed");
            }
            else if(cmd[0].equals("quit")){
                return(-1);
            }
            else{
                System.out.println("Invalid command");
            }
            long EndTime=System.currentTimeMillis();
            long Duration=EndTime-StartTime;
            System.out.println("STAT INFO: Operation Finished in "+Duration+" msec.");
            return(0);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return(-1);
    }
}
