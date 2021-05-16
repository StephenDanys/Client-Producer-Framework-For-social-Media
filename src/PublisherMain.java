import Nodes.Publisher;
import channelName.ChannelName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

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
        //adding predifined port numbers for brokers
        ArrayList<Integer> serverPorts=new ArrayList<>();
        serverPorts.add(101);
        serverPorts.add(102);
        serverPorts.add(103);

        //port
        int port = Integer.parseInt(args[1]);
        ChannelName name= new ChannelName("Publisher" + port, null);
        Publisher pub= new Publisher(port,null,name);
        if (pub.init(serverPorts)){
            pub.connect();
        }
    }
}
