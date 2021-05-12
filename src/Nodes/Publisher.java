package Nodes;

import Extras.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;

public class Publisher implements Node{
    private static final int PORT = 2001;
    private final String IP;
    private final String RANGE; //range of artists (regex expression)

    private ServerSocket server;
    ChannelName channelName;

    //constructor
    public Publisher(String ip, String range){
        IP = ip;
        RANGE = range;
        Utilities.print("PUBLISHER: Initialize publisher");
    }

    //methods
    public void  addHashTag(String tag){

    }
    public void  removeHashTag(String tag){

    }
    public void getBrokerList(){

    }
    public Broker hashTopic(String topic){

    }
    public void push(String topic, Value value){

    }
    public void notifyFailure(Broker b){

    }
    public void notifyBrokersForHashTags(String Tag){

    }
    public ArrayList<Value> generateChunks(String v){

    }
    //methods from Node
    public void init(int x){}
    public List<Broker> getBrokers(){}
    public void connect(){}

    public void disconnect(){
        Utilities.print("PUBLISHER: Close socket connection");

        if (socket != null){
            try {
                socket.close();
            } catch (IOException e) {
                Utilities.printError("PUBLISHER: ERROR: Could not close socket connection");
            }
        }
    }
    public void updateNodes(){}
}
