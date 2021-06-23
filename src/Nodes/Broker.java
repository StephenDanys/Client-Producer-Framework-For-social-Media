package Nodes;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import Extras.*;
import VideoFile.VideoFile;
import FileHandler.*;


public class Broker {
    //class variables
    //server ID
    private final String IP = "127.0.0.1";
    private final int port; //for publishers and brokers
    private final int consumerPort; // for consumers and Identification
    private final BigInteger HASH_VALUE; //hash value for servers

    //sockets
    private ServerSocket pubSocket; //publisher and broker server
    private ServerSocket conSocket; //consumer socket

    //Data Structures
    private ArrayList<Integer> brokersList; //the list with the available brokers (Ports)
    private HashMap<Integer, String> registeredPublishers = new HashMap<>(); //list with the registered publishers( Ports)
    private HashMap<String, Integer> hashTagFromPublisher; //Maps each topic to responsible publisher Port
    private HashMap<String,Integer> hashTagToBrokers; //Maps Topic to the according brokers (uses Consumer Ports)


    private static HashMap<Pair<String, BigInteger>, Pair<String, Integer>> registeredUsers;
    private static File userFile; // registered users
    private final ExecutorService threadPool;

    //constructor for class Broker
    public Broker (int port){
        Extras.print("BROKER: Broker Constructor" );
        this.HASH_VALUE = Extras.SHA1(IP + port);
        hashTagFromPublisher = new HashMap<>();
        hashTagToBrokers = new HashMap<>();
        threadPool = Executors.newCachedThreadPool();
        this.port=port;
        this.consumerPort=port %100 ; //so for port 101 consumer port is 01
    }

    //initialize all available brokers
    public void init(ArrayList<Integer> brokerPorts){
        Extras.print("BROKER: Initializing Broker.");
        brokersList = brokerPorts;
        //create file with user credentials
        //userFile = FileHandler.createUserFile();

        //read user credentials
        //registeredUsers = FileHandler.readUsers(userFile);
    }

    //make the broker online. Wait for a connection
    public void connect() {
        Extras.print("BROKER: Make broker online");

        pubConnection();
        conConnection();
    }

    //@return the hashTags and their brokerPorts
    public HashMap<String,Integer> getBrokers(){
        return hashTagToBrokers;
    }

    //@returns the map from each hashtag to a publisher
    public HashMap<String, Integer> getPublishers(){
        return hashTagFromPublisher;
    }

    //@return the IP number
    public String getIP(){
        return IP;
    }

    //@return the value for the hashing procedure
    public BigInteger getValue(){
        return HASH_VALUE;
    }

    /**
     Activates the broker in the network. Makes him visible from other brokers and publishers.
     Creates a thread to accept incoming connections and creates a thread for each created
     connection. Uses the acceptPublisher(Publisher p) method.

     the port used in the pubSocket is for this instance of broker, since the publishers will know
     all of the brokers' ports
      */
    public void pubConnection(){
        Extras.print("BROKER: making the broker active online for publishers and other brokers.");
        try{
            pubSocket = new ServerSocket(port); //creating a new serverSocket for the publisher
        } catch(IOException e){
            Extras.printError("BROKER: ERROR: could not go online for publishers/brokers!");

            try{
                if(pubSocket != null){
                    pubSocket.close();
                }
            } catch (IOException ex){
                Extras.printError("BROKER: ERROR: Server could not shut down");
            } 
        }

        //creating a thread to await for a connection from publishers or other Brokers
        Thread connect = new Thread(new Runnable(){

            public void run(){
                while(true){
                    Socket connection;
                    try{
                        connection = pubSocket.accept();

                        //creation of another thread to process the connection
                        Thread conProcess = new Thread(new Runnable(){

                            public void run(){
                                //get connected with the socket with the specific port
                                ObjectInputStream  in = null;
                                int clientPort=0;
                                try {
                                    in = new ObjectInputStream(connection.getInputStream());
                                    clientPort = (int) in.readObject();
                                } catch (IOException | ClassNotFoundException e) {
                                    e.printStackTrace();
                                }


                                //CASE 1
                                //Socket belongs to other Broker
                                if(brokersList.contains(clientPort)){
                                    acceptBrokerConnection(connection, in, clientPort);
                                    return;
                                }

                                //Case 2
                                //Socket belongs to publisher
                                acceptPublisher(connection, in, clientPort);
                            }
                        });
                        threadPool.execute(conProcess);
                    } catch (IOException e) {
                        Extras.printError("BROKER: ERROR: Problem connecting to Publisher!");
                    }
                }
            }
        });
        threadPool.execute(connect);
    }

    /**Activates the broker in the network. Makes him visible from the consumers.
      Creates a thread to accept incoming connections and creates a thread for each created 
      connection. Uses the accept(Consumer s) method.
      Connect with broker and gain access to his consumers
      =>we need this to update our files on otherBrokers
    */
    public void acceptBrokerConnection(Socket connection, ObjectInputStream input, int brokerPort){
        Extras.print("BROKER: Accepting broker connection!");
        ArrayList<String> topics;
        try {
            ObjectInputStream in=input;
            topics= (ArrayList<String>) in.readObject();
            disconnect(connection);
        } catch (IOException e) {
            Extras.printError("BROKER: ERROR: Could not read from the stream!");
            disconnect(connection);
            return;
        } catch (ClassNotFoundException e) {
            Extras.printError("BROKER: ERROR: Cannot cast object to List!");
            disconnect(connection);
            return;
        }

        setOuterConsumerSource(topics, brokerPort %100);
    }

    /**
     *add the broker and the consumers he is responsible for in the hashMap
     * remove everything that maps to the other broker, then add the right topics
     */
    private synchronized void setOuterConsumerSource(List<String> hashTag, int broker){

        for (String topic : hashTagToBrokers.keySet()){
            if(hashTagToBrokers.get(topic)==broker) hashTagToBrokers.remove(topic);
        }

        for (String hash : hashTag) {
            hashTagToBrokers.put(hash, broker);
        }
    }

    /*If publisher is registered fetch the topics.
     * In other case send has value.
     * @param connection socket for connection*/
    public synchronized void acceptPublisher(Socket connection,ObjectInputStream input, int pubPort){
        Extras.print("BROKER: Accept publisher connection");

        ObjectOutputStream out;
        // CASE 1
        // Publisher is registered
        if(registeredPublishers.keySet().contains(pubPort)){
            ArrayList<String> pubs;
            //wait for hashtags
            try{
                ObjectInputStream in= input;
                pubs =(ArrayList<String>) in.readObject();
            } catch (IOException e) {
                e.printStackTrace();
                Extras.printError("BROKER: ACCEPT PUBLISHER CONNECTION: Could not read from stream");
                return;
            } catch (ClassNotFoundException e) {
                Extras.printError("BROKER: ACCEPT PUBLISHER CONNECTION: Could not cast Object to ArrayList");
                return;
            }
            //save the hashtags
            setInnerPublisherSource(pubs, pubPort);
            //close connection
            disconnect(connection);

            //send info to other brokers
            ArrayList<String> bros= new ArrayList<>();
            for (String topic: hashTagToBrokers.keySet()){
                if (hashTagToBrokers.get(topic)==getConsumerPort()) bros.add(topic);
            }
            notifyBrokersOnChanges(bros);
            return;
        }

        //Case 2
        //Publisher is not registered
        try{
        ObjectInputStream in = input;
        String chName = (String) in.readObject();
        registerPublisher(pubPort, chName);

        //send your hash code

            out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject(getValue());
            out.flush();

            disconnect(connection);
        } catch (IOException | ClassNotFoundException e) {
            Extras.printError("BROKER: ACCEPT PUBLISHER CONNECTION: ERROR: Problem with output stream");
            disconnect(connection);
        }
    }

    /**
     * Write new hashtag in list Keep from which publisher the hash was fetched
     * Also remove topics that might not exist anymore
     */
    public synchronized void setInnerPublisherSource(ArrayList<String > topics, int publisher) {
        for (String topic : topics) {
            if(!hashTagToBrokers.keySet().contains(topic)) hashTagToBrokers.put(topic, getConsumerPort());
            if (!hashTagFromPublisher.keySet().contains(topic))hashTagFromPublisher.put(topic, publisher);
        }

        for(String hash: hashTagFromPublisher.keySet()) {
            if (hashTagFromPublisher.get(hash) == publisher)
                if(!topics.contains(hash)) hashTagFromPublisher.remove(hash);
        }

        for(String hash : hashTagToBrokers.keySet()){
            if(!hashTagFromPublisher.keySet().contains(hash)) hashTagToBrokers.remove(hash);
        }
    }

    /**
     * Send to other brokers info
     * @param topics and updated list of topics that this broker is responsible for
     */
    public void notifyBrokersOnChanges(ArrayList<String > topics){
        Extras.print("BROKER: Notifiyng other brokers!");

        for(Integer broker :brokersList){
            if(!broker.equals(getPort())){ //if the broker is not the current one

                Thread notify = new Thread(new Runnable(){
                    public void run(){
                        Socket socket;
                        ObjectOutputStream out;
                        //try until you get it
                        while (true){
                            try{
                                socket = new Socket(IP, broker); //open connection
                                out = new ObjectOutputStream(socket.getOutputStream());
                                out.writeObject(getPort()); //send to other brokers my port number
                                out.flush();
                                out.writeObject(topics);
                                out.flush();

                                disconnect(socket);
                                break;
                            } catch (IOException e){
                                Extras.printError("BROKER: Error notifiyng other brokers!");
                            }
                        }
                    }
                });
                threadPool.execute(notify);
            }
        }
    }

    //add the publisher in broker's registered publisher list
    private synchronized void registerPublisher(Integer pubPort, String chName) {
        registeredPublishers.put(pubPort, chName);
    }

    public void conConnection(){
        Extras.print("BROKER: Making broker online for consumers!");

        try{
            conSocket = new ServerSocket(consumerPort);
        } catch (IOException e) {
            Extras.printError("BROKER: ERROR: Could not go online for consumers");
            try {
                if (conSocket != null)
                    conSocket.close();
            } catch (IOException ex) {
                Extras.printError("BROKER: TO CLIENT CONNECTION: ERROR: Server could not shut down");
            }
        }

        Thread con_task = new Thread(new Runnable() {
            public void run(){
                while(true){
                    try{
                        Socket connection = conSocket.accept();

                        //create a thread to process the connection
                        Thread con_task = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                acceptConsumer(connection);
                                try{
                                    connection.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                        threadPool.execute(con_task);
                    } catch (IOException e){
                        Extras.printError("BROKER: TO CLIENT CONNECTION: ERROR: Problem connecting");
                    }
                }
            }
        });
        threadPool.execute(con_task);
    }

    //Connect with the consumer and process the request
    public void acceptConsumer(Socket connection) {
        Extras.print("BROKER: Accept consumer connection");

        ObjectInputStream in;
        try{
            in = new ObjectInputStream(connection.getInputStream());
            String request = (String) in.readObject();

            switch (request) {
                /*case "REGISTER":
                    registerUser(connection);
                    break;
                case "LOGIN":
                    loginUser(connection);
                    break;*/
                case "PULL":
                    String title = (String) in.readObject();
                    String topic = (String) in.readObject();
                    pull(connection, title, topic);
                    break;
                case "INIT": //send initial data to client
                    sendInitData(connection);
            }
        } catch (IOException e) {
            Extras.printError("BROKER: ACCEPT CONSUMER CONNECTION: Could not read from stream");
        } catch (ClassNotFoundException e) {
            Extras.printError("BROKER: ACCEPT PUBLISHER CONNECTION: Could not cast Object to Pair");
        }
    }
    /**
     * Method that helps initialize the Consumer
     * @param clientCon socket to client
     */
    public void sendInitData(Socket clientCon){
        Extras.print("BROKER: Send Initial Info to server");
        if(registeredPublishers==null){
            try{
                // inform consumer that you can't find the topic
                ObjectOutputStream cli_out = new ObjectOutputStream(clientCon.getOutputStream());
                cli_out.writeObject("FAILURE");
                cli_out.flush();
                disconnect(clientCon);
            } catch (IOException e) {
                Extras.printError("BROKER: INIT: ERROR: Could not use out stream");
            }
        } else {
            try{
                ObjectOutputStream cli_out= new ObjectOutputStream((clientCon.getOutputStream()));
                cli_out.writeObject(registeredPublishers.values());
                cli_out.flush();

                cli_out.writeObject(hashTagToBrokers);
                cli_out.flush();
            } catch (IOException e) {
                Extras.printError("BROKER: INIT: ERROR: Could not use out stream");
            }
        }
    }
    /**
     * @param clientCon socket to client
     * @param title video title
     * if title is null pull returns all videos associated with topic
     * @param topic hashtag or channel name
     */
    public void pull (Socket clientCon,String title, String topic){
        Extras.print("BROKER: Get requested video from client");
        int broker = hashTagToBrokers.get(topic); // get broker port responsible for that topic
        Extras.print(String.valueOf(broker));
        if(broker ==0){
            try{
                // inform consumer that you can't find the topic
                ObjectOutputStream cli_out = new ObjectOutputStream(clientCon.getOutputStream());
                cli_out.writeObject("FAILURE");
                cli_out.flush();
                disconnect(clientCon);
            } catch (IOException e) {
                Extras.printError("BROKER: PULL: ERROR: Could not use out stream");
            }

        } else if (broker == getConsumerPort()){ // the current broker is responsible for this topic
            int publisher = hashTagFromPublisher.get(topic); //we need to go to to this publisher port
            try{
                //inform consumer that you are about to send him the video
                ObjectOutputStream cli_out = new ObjectOutputStream(clientCon.getOutputStream());
                cli_out.writeObject("ACCEPT");
                cli_out.flush();

                Socket pubConnection = new Socket(IP, publisher);

                //send request for music file to publisher
                ObjectOutputStream pub_out = new ObjectOutputStream(pubConnection.getOutputStream());
                Pair<String,String> search= new Pair<String, String>(topic,title);
                pub_out.writeObject(search);
                pub_out.flush();

                // get files from publisher
                ObjectInputStream pubIn = new ObjectInputStream(pubConnection.getInputStream());
                int counter = 0;
                VideoFile file;
                while (counter < 2) {
                    try {
                        while ((file = (VideoFile) pubIn.readObject()) != null) {
                            // send file chunks back to consumer
                            cli_out.writeObject(file);
                            cli_out.flush();
                            counter = 0;
                        }
                        cli_out.writeObject(null);
                        cli_out.flush();
                        ++counter;
                    } catch (EOFException e) {
                        Extras.printError("BROKER: pull : inconsistency in sending files to client");
                    }
                }

                disconnect(pubConnection);
            } catch (IOException e) {
                Extras.printError("BROKER: PULL: Could not use streams");
            } catch (ClassNotFoundException e) {
                Extras.printError("BROKER: pull: Could not cast Object to MusicFile");
            }
        } else { //the current broker is not responsible for this publisher
            try{
                //inform consumer that you will send brokers
                ObjectOutputStream clientOut = new ObjectOutputStream(clientCon.getOutputStream());
                clientOut.writeObject("DECLINE");
                clientOut.flush();

                clientOut.writeObject(hashTagToBrokers);
                clientOut.flush();
            } catch (IOException e) {
                Extras.printError("BROKER: PULL: Could not use streams");
            }
            disconnect(clientCon);
        }
    }

    /*
    /**
     * Save credentials in a file and inform user about his registration
     *
     * @param connection socket for connection

    private void registerUser(Socket connection) {
        Extras.print("BROKER: Register user");

        try {
            ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(connection.getInputStream());

            Pair<String, BigInteger> credentials = (Pair<String, BigInteger>) in.readObject();
            Pair<String, Integer> extra = (Pair<String, Integer>) in.readObject();



            String message = FileHandler.writeUser(userFile, credentials, extra, registeredUsers);

            out.writeObject(message);
            out.flush();
        } catch (IOException e) {
            Extras.printError("BROKER: REGISTER USER: Could not use streams");
        } catch (ClassNotFoundException e) {
            Extras.printError("BROKER: ACCEPT PUBLISHER CONNECTION: Could not cast Object to Pair");
        }

        disconnect(connection);
    }
    /**
     * Check credentials send by user and inform user about their validity
     * @param connection socket for connection

    private synchronized void loginUser(Socket connection) {
        Extras.print("BROKER: Log in user");

        try {
            ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
            Pair<String, BigInteger> credentials = (Pair<String, BigInteger>) in.readObject();

            boolean registered = false;
            Pair<String, BigInteger> client = null;
            for (Pair<String, BigInteger> consumer : registeredUsers.keySet()) {// check if consumer is registered
                if (consumer.getKey().equals(credentials.getKey())) { // check his username
                    registered = true;
                    if (credentials.getValue().equals(consumer.getValue())) { // check his password
                        client = consumer;
                    }
                }
            }

            // send message to consumer
            ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
            String message = "FALSE"; // used for wrong credentials
            if (!registered) { // if not registered, sign consumer up
                message = "REGISTER";
            } else if (client != null) { // if consumer registered and used right credentials
                message = "VERIFIED";
            }
            out.writeObject(message);
            out.flush();

            disconnect(connection);
        } catch (IOException e) {
            Extras.printError("BROKER: LOGIN USER: Could not use streams");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            Extras.printError("BROKER: LOGIN USER: Could not cast Object to Pair");
            e.printStackTrace();
        }
    }
    */
    public void disconnect (Socket socket){
        Extras.print("BROKER: Close socket connection");

        if (socket != null){
            try {
                socket.close();
            } catch (IOException e) {
                Extras.printError("BROKER: ERROR: Could not close socket connection");
            }
        }
    }

    private int getPort() { return this.port; }
    private int getConsumerPort(){return  consumerPort;}

    public String toString(){
        return "Broker@"+getIP()+"@"+getPort()+"@"+getValue();
    }
}
