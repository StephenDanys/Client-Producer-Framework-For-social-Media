import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.*;
import Nodes.Broker;
import Extras.Extras;

public class BrokerMain3 {
    private static ArrayList<Integer> brokerPorts;

    public static void main(String[] args){
        String IP;
        try {
            IP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.err.println("ERROR: Consumer down");
            System.err.println("ERROR: Could not get IP address");
            return;
        }
        brokerPorts= new ArrayList<>();
        brokerPorts.add(101);
        brokerPorts.add(102);
        brokerPorts.add(103);

        int PORT = Integer.parseInt(args[0]);
        if(!brokerPorts.contains(PORT)){
            System.out.println("Invalid port number");
            System.exit(1);
        }
        Broker example = new Broker(PORT);
        example.init( brokerPorts);
        example.connect();
    }//main

}//class

