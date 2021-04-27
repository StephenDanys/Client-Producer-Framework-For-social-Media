import java.util.*;

public class Broker  implements Node{
    //class variables
    List<Consumer> registeredUsers;
    List<Publisher> registeredPublishers;

    //Broker Methods
    public void calculateKeys(){

    }
    public Consumer acceptConnection(Consumer c) {

    }
    public Publisher acceptPublisher(Publisher p){

    }
    public void notifyBrokersOnChanges(){

    }
    public void pull (String s){

    }
    public void filterConsumers(){

    }
    //Node Methods Implemetation
    public void init(int x){}
    public List<Broker> getBrokers(){}
    public void connect(){}
    public void disconnect(){}
    public void updateNodes(){}
}
