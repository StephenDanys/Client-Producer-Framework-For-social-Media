import java.util.*;

public class Publisher implements Node{
    ChannelName channelName;

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
    public void disconnect(){}
    public void updateNodes(){}
}
