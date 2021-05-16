import Nodes.Publisher;
import channelName.ChannelName;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class PublisherMain2 {
    public static void main(String[] args){

        String IP;
        try{
            IP= InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e){
            System.err.println("ERROR: Publisher down");
            System.err.println("ERROR: Could not get IP address");
            return;
        }
        //adding predifined port numbers for brokers
        ArrayList<Integer> serverPorts=new ArrayList<>();
        serverPorts.add(101);
        serverPorts.add(102);
        serverPorts.add(103);

        //port
        int port = Integer.parseInt(args[0]);
        ChannelName name= new ChannelName("Publisher" + port, new HashSet<String>());
        Publisher pub= new Publisher(port,null,name);
        if (pub.init(serverPorts)){
            pub.connect();
        }
    }
}

