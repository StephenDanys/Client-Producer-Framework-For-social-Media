package Nodes;

import Extras.Extras;
import Nodes.Broker;
import supportClasses.ChannelName;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

public class Publisher implements Node{
    private  final int port;
    private static final  String IP= "127.0. 0.1";
    private final String RANGE; //range of artists (regex expression)

    private ServerSocket server;
    ChannelName channelName;

    //constructor
    public Publisher(int port, String range, ChannelName name){
        this.port=port;
        RANGE = range;
        Extras.print("PUBLISHER: Initialize publisher");
        this.channelName= name;
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
        Extras.print("PUBLISHER: Close socket connection");

        if (socket != null){
            try {
                socket.close();
            } catch (IOException e) {
                Extras.printError("PUBLISHER: ERROR: Could not close socket connection");
            }
        }
    }
    public void updateNodes(){}
}
