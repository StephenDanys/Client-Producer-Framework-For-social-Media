import Nodes.Publisher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PublisherMain {
    public static void main(String[] args){
        String IP;
        try{
            IP= InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e){
            System.err.println("ERROR: Publisher down");
            System.err.println("ERROR: Could not get IP address");
            return;
        }
        if (!IP.equals("127.0.0.1")) {
            System.err.println("ERROR: Publisher down");
            System.err.println("ERROR: IP is loopback address");
            return;
        }
        Publisher pub=new Publisher();
        List<String> serverPorts=null;
        if(args.length < 1){
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while(true){
                System.out.println("Server ports (split with spases): ");
                try{
                    String[] input = reader.readLine().split(" ");
                    serverPorts = new ArrayList<>(Arrays.asList(input));
                    break;
                }catch(IOException e){
                    System.err.println("ERROR: Could not read input");
                }
            }
        }else{
            serverPorts = new ArrayList<>(Arrays.asList(args));
        }

        if (pub.init(serverPorts)){
            pub.online();
        }
    }
}
