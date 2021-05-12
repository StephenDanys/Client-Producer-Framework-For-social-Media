package Nodes;

import java.util.*;
import java.net.*;
import java.math.BigInteger;
import java.util.*;
impoe


public class Broker {
    //class variables
    private static final int PUB_PORT = 1999; //the port numner for the contect creators and the other broker nodes. 
    private static final int CON_PORT = 2000; //the port number for the consumers.
    private final String IP;
    private final BigInteger HASH_VALUE;

    private ServerSocket pubSocket; //publisher and broker server
    private ServerSocket conSocket; //consumer socket 

    private List<Broker> brokersList; //the list with the available brokers
    private List<Publisher> registeredPublishers = new List();
    private List<Consumer> registeredUsers = new List();
    private static HashMap<Pair<String, BigInteger>, Pair<String, Integer>> registeredUsers;

    private static File userFile; //registered users
    
    private HashMap<Publisher,String> consumersToPublishers; //assigns the consumers to the according publishers 
    private HashMap<Consumer,String> consumersToBrokers; //assigns the consumers to the accoridng 
    
    private final ExecutorService threadPool;

    //constructor for class Broker
    public Broker (String IP){

        System.out.println("BROKER: Broker Constructor");
        this.IP = IP;
        this.HASH_VALUE = Utilities.SHA1(IP + PUB_PORT);
        consumersToPublishers = new HashMap<>();
        consumersToBrokers = new HashMap<>();
        threadPool = Executors.newCachedThreadPool();

    }

    //initializa all available brokers
    public void init(List<Broker> brokerIPs){

        System.out.println("BROKER: Initializing Broker.");
        brokersList = brokerIPs;

        //create file with user credentials
        userFile = FileHandler.createUserFile();

        //read user credentials
        registeredUsers = FileHandler.readUsers(userFile);

    }

    //make the broker online. Wait for a connection
    public void connect() {
        System.out.println("BROKER: Make broker online");

        pubConnection();
        conConnection();
    }

    //get Functions @return the classes attributes
    
    //@return the broker Ip's and the consumers they are connected to
    public HashMap<Consumer,String> getBorkers(){
        return consumersToBrokers;
    }

    //@returns the matching between the publishers and consumers 
    public HashMap<Publisher,String> getPublishers(){
        return consumersToPublishers;
    }

    //@return the port assigned for the publishers
    public static int getPubPort(){
        return PUB_PORT;
    }

    //@return the port assigned for the consumers
    public static int getConPort(){
        return CON_PORT;
    }

    //@return the IP number
    public String getIP(){
        return IP;
    }

    //@return the value for the hashing procedure
    public BigInteger getValue(){
        return HASH_VALUE;
    }

    /*Activates the broker in the network. Makes him visible from other brokers and publishers. 
      Creates a thread to accept incoming connections and creates a thread for each created 
      connection. Uses the acceptPublisher(Publisher p) method.*/
    
    public void pubConnection(){

        System.out.println("BROKER: making the broker active online for publishers and other brokers.");

        try{
            pubSocket = new ServerSocket(PUB_PORT);
        } catch(IOException e){
            System.out.println("BROKER: ERROR: could not go online for publishers/brokers!");

            try{
                if(pubSocket != null){
                    pubSocket.close();
                }
            } catch (IOException ex){
                System.out.println("BROKER: ERROR: Server could not shut down");
            } 
        }

        //creating a thread to await for a connection from publishers or other Brokers
        Thread connect = new Thread(new Runnable(){

            public void run(){
                while(true){
                    final Socket connection;
                    try{
                        connection = pubSocket.accept();

                        //creation of another thread to procees the connection
                        Thread conProcess = new Thread(new Runnable(){

                            public void run(){
                                String clientIP = connection.getInetAddress().getHostAddress();

                                if(brokersList.contains(clientIP)){
                                    acceptBrokerConnection(connection,clientIP);
                                    return;
                                }

                                acceptPublisher(connection);
                            }
                        });
                        threadPool.execute(conProcess);
                    } catch (IOException e) {
                        System.out.println("BROKER: ERROR: Problem connecting to Publisher!");
                    }
                }
            }
        });
        threadPool.execute(connect);
    }

    /*Activates the broker in the network. Makes him visible from the consumers. 
      Creates a thread to accept incoming connections and creates a thread for each created 
      connection. Uses the accept(Consumer s) method.*/
    
    public void conConnection(){
        System.out.println("BROKER: Making broker online for consumers!");

        try{
            conSocket = new ServerSocket(CON_PORT);
        } catch (IOException e) {
            System.out.println("BROKER: ERROR: Could not go online for consumers");
            try {
                if (conSocket != null)
                    conSocket.close();
            } catch (IOException ex) {
                System.out.println("BROKER: TO CLIENT CONNECTION: ERROR: Server could not shut down");
            }
        }

        Thread con_task = new Thread(new Runnable() {
            public void run(){
                String clientIP = conSocket.getInetAddress().getHostAddress();
                acceptConsumer(conSocket);
            }
        });
        threadPool.execute(con_task);
    }

    /*Connect with broker and gain access to his consumers*/
    public void acceptBrokerConnection(Socket connection, String brokerIP){
        System.out.println("BROKER: Accepting broker connection!");

        ObjectInputStream in;
        Consumer consumers;
        
        try {
            in = new ObjectInputStream(connection.getInputStream());
            consumers = (Consumer) in.readObject();
            disconnect(connection);
        } catch (IOException e) {
            System.out.println("BROKER: ERROR: Could not read from the stream!");
            disconnect(connection);
            return;
        } catch (ClassNotFoundException e) {
            System.out.println("BROKER: ERROR: Cannot cast object to List!");
            disconnect(connection);
            return;
        }

        setOuterConsumerSource(consumers, brokerIP);
    }

    //add the broker and the consumers he is responsible for in the hashMap
    private synchronized void setOuterConsumerSource(List<Consumer> consumers, String broker) {
        for (Consumer consumer : consumers) {
            consumersToBrokers.put(consumer, broker);
        }
    }

    //

    //Broker Methods
    public void calculateKeys(){}

    //Connect with the consumer and process the request
    public Consumer acceptConsumer(ServerSocket connection) {


    }
    public Publisher acceptPublisher(Publisher p){}

    //this function sends this broker's consumers to the other brokers
    public void notifyBrokersOnChanges(List<Broker> brokers){
        System.out.println("BROKER: Notifiyng other brokers!");

        for(Broker broker : brokers){
            if(!broker.equals(getIP())){
                Thread notify = new Thread(new Runnable(){
                    public void run(){
                        Socket socket;
                        ObjectOutputStream out;

                        while (true){
                            try{
                                socket = new Socket(socket,PUB_PORT);
                                out = new ObjectOutputStream(socket.getOutputStream());
                                out.writeObject(brokers);
                                out.flush();

                                disconnect(socket);
                                break;
                            } catch (IOException e){
                                System.out.println("BROKER: Error notifiyng other brokers!");
                            }
                        }
                    }
                });
                threadPool.execute(notify);
            }
        }
    }

    //add the publisher in broker's registered publisher list
    private synchronized void registerPublisher(Publisher clientIP) {
        registeredPublishers.add(clientIP);
    }

    public void pull (String s){}
    public void filterConsumers(){}


    //Node Methods Implemetation
    public void init(int x){}

    public List<Broker> getBrokers(){
        return Brokers;
    }
    
    public void updateNodes(){}

    public void disconnect (Socket socket){
        System.out.println("BROKER: Close socket connection");

        if (socket != null){
            try {
                socket.close();
            } catch (IOException e) {
                System.out.println("BROKER: ERROR: Could not close socket connection");
            }
        }
    }

    
    public String toString(){
        return "Broker@"+getIP()+"@"+getPubPort()+"@"+getConPort()+"@"+getValue();
    }
}
