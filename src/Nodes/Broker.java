package Nodes;

import javax.imageio.plugins.tiff.ExifGPSTagSet;
import javax.sound.sampled.Port;
import java.lang.reflect.Array;


import java.math.BigInteger;
import java.io.IOException;
import java.util.function.Consumer;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import Extras.Extras;


public class Broker {
    //class variables
    private static int pubPort; //port for publishers
    private static int conPort; // port for consumers
    private final String IP = "127.0.0.1";
    private final BigInteger HASH_VALUE;

    private ServerSocket pubSocket; //publisher and broker server
    private ServerSocket conSocket; //consumer socket 

    private ArrayList<Integer> brokersList; //the list with the available brokers
    private List<Integer> registeredPublishers = new ArrayList<Integer>(); //list with the registered publishers
    private List<Integer> registeredUsers = new ArrayList<Integer>(); //list with the registered consumers
    //private static HashMap<Integer, BigInteger> registeredUsers;

    private static File userFile; //registered users
    
    private HashMap<Integer,Integer> consumersToPublishers; //assigns the consumers to the according publishers
    private HashMap<Integer,Integer> consumersToBrokers; //assigns the consumers to the according brokers
    
    private final ExecutorService threadPool;

    //constructor for class Broker
    public Broker (int port){

        Extras.print("BROKER: Broker Constructor");
        this.HASH_VALUE = Extras.SHA1(IP + pubPort);
        consumersToPublishers = new HashMap<>();
        consumersToBrokers = new HashMap<>();
        threadPool = Executors.newCachedThreadPool();
        this.pubPort = port;
        this.conPort = port;
    }

    //initialize all available brokers
    public void init(ArrayList<Integer> brokerPorts){

        Extras.print("BROKER: Initializing Broker.");
        brokersList = brokerPorts;

        //create file with user credentials
        userFile = FileHandler.createUserFile();

        //read user credentials
        registeredUsers = FileHandler.readUsers(userFile);

    }

    //make the broker online. Wait for a connection
    public void connect() {
        Extras.print("BROKER: Make broker online");

        pubConnection();
        conConnection();
    }

    //get Functions @return the classes attributes
    
    //@return the broker ports and the consumers they are connected to
    public HashMap<Integer,Integer> getBrokers(){
        return consumersToBrokers;
    }

    //@returns the matching between the publishers and consumers 
    public HashMap<Integer,Integer> getPublishers(){
        return consumersToPublishers;
    }

    //@return the assigned port for the publishers
    public static int getPubPort(){
        return pubPort;
    }

    //@return the assigned port for the consumers
    public static int getConPort() {return conPort;}

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

        Extras.print("BROKER: making the broker active online for publishers and other brokers.");

        try{
            pubSocket = new ServerSocket(pubPort); //creatingg a new serverSocket for the publisher
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
                                int clientPort = connection.getLocalPort();

                                //CASE 1
                                //Socket belongs to other Broker
                                if(brokersList.contains(clientPort)){
                                    acceptBrokerConnection(connection,clientPort);
                                    return;
                                }

                                //Case 2
                                //Socket belongs to publisher
                                acceptPublisher(connection);
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

    /*Activates the broker in the network. Makes him visible from the consumers. 
      Creates a thread to accept incoming connections and creates a thread for each created 
      connection. Uses the accept(Consumer s) method.*/
    
    public void conConnection(){
        Extras.print("BROKER: Making broker online for consumers!");

        try{
            conSocket = new ServerSocket(conPort);
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
                                int clientPort = connection.getLocalPort();

                                acceptConsumer(connection, clientPort);

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

    /*Connect with broker and gain access to his consumers*/
    public void acceptBrokerConnection(Socket connection, int brokerPort){
        Extras.print("BROKER: Accepting broker connection!");

        ObjectInputStream in;
        ArrayList<Consumer> consumers;
        
        try {
            in = new ObjectInputStream(connection.getInputStream());
            consumers = (ArrayList<Consumer>) in.readObject();
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

        setOuterConsumerSource(consumers, brokerPort);
    }

    //add the broker and the consumers he is responsible for in the hashMap
    private synchronized void setOuterConsumerSource(List<Integer> consumers, int broker) {
        for (Integer consumer : consumers) {
            consumersToBrokers.put(consumer, broker);
        }
    }

    //Connect with the consumer and process the request
    public void acceptConsumer(Socket connection, int clientPort) {
        Extras.print("BROKER: Accept consumer connection");

        ObjectInputStream in;
        try{
            in = new ObjectInputStream(connection.getInputStream());
            String request = (String) in.readObject();

            switch (request) {
                case "REGISTER":
                    registerUser(connection, clientPort);
                    break;
                case "PULL":
                    Pair<String, String> song = (Pair) in.readObject();
                    pull(connection, song.getKey(), song.getValue());
                    break;
                case "INIT":
                    pull(connection, null, "_INIT");
                    break;
            }
        } catch (IOException e) {
            Extras.printError("BROKER: ACCEPT CONSUMER CONNECTION: Could not read from stream");
        } catch (ClassNotFoundException e) {
            Extras.printError("BROKER: ACCEPT PUBLISHER CONNECTION: Could not cast Object to Pair");
        }
    }

    private void registerUser(Socket connection, int clientPort){
        Extras.print("BROKER: Register user");

        try{
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

    /*If publisher is registered fetch the videos.
    * In other case send has value.
    * @param connection socket for connection*/
    public void acceptPublisher(Socket connection){
        Extras.print("BROKER: Accept publisher connection");

        ObjectOutputStream out;
        ObjectInputStream in;
        int clientPort = connection.getLocalPort();

        // CASE 1
        // Publisher is registered

        if(registeredPublishers.contains(clientPort)){
            ArrayList<Integer> pubs;

            //wait for publishers
            try{
                in = new ObjectInputStream(connection.getInputStream());
                pubs = (ArrayList<Integer>) in.readObject();
            } catch (IOException e) {
                Extras.printError("BROKER: ACCEPT PUBLISHER CONNECTION: Could not read from stream");
                return;
            } catch (ClassNotFoundException e) {
                Extras.printError("BROKER: ACCEPT PUBLISHER CONNECTION: Could not cast Object to ArrayList");
                return;
            }

            //save the publishers
            setInnerPublisherSource(pubs, clientPort);

            //close connection
            disconnect(connection);

            //send info to other brokers
            notifyBrokersOnChanges(pubs);
            return;
        }

        //Case 2
        //Publisher is not registered
        registerPublisher(clientPort);
        //send broker hashes

        //send your hash code
        try{
            out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject(getValue());
            out.flush();

            disconnect(connection);
        } catch (IOException e) {
            Extras.printError("BROKER: ACCEPT PUBLISHER CONNECTION: ERROR: Problem with output stream");
            disconnect(connection);
        }
    }

    /**
     * Write new publisher in list Keep from which publisher the artist was fetched
     */
    public synchronized void setInnerPublisherSource(ArrayList<Integer> publishers, int publisher) {
        for (int pub : publishers) {
            consumersToBrokers.put(pub, getPubPort());
            consumersToPublishers.put(pub, publisher);
        }
    }


    //this function sends this broker's consumers to the other brokers
    public void notifyBrokersOnChanges(List<Integer> brokers){
        Extras.print("BROKER: Notifiyng other brokers!");

        for(Integer broker : brokers){
            if(!broker.equals(getPubPort())){
                Thread notify = new Thread(new Runnable(){
                    public void run(){
                        Socket socket = new;
                        ObjectOutputStream out;
                        //try until you get it
                        while (true){
                            try{
                                socket = new Socket(socket, pubPort); //open connection
                                out = new ObjectOutputStream(socket.getOutputStream());
                                out.writeObject(brokers);
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
    private synchronized void registerPublisher(Integer clientPort) {
        registeredPublishers.add(clientPort);
    }

    /*Get Requested video from user <title,port>*/
    public void pull (Socket clientCon,String title, int port){
        Extras.print("BROKER: Get requested song from user");

        int broker = consumersToBrokers.get(port); // get broker port responsible for that artist

        if (port == 0) { // consumer wants the list of brokers
            broker = 0;
        }

        if(broker == 0){
            try{
                // inform consumer that you will send the brokers
                ObjectOutputStream cli_out = new ObjectOutputStream(clientCon.getOutputStream());
                cli_out.writeObject("FAILURE");
                cli_out.flush();

                disconnect(clientCon);
            } catch (IOException e) {
                Extras.printError("BROKER: PULL: ERROR: Could not use out stream");
            }
        } else if (broker == getPubPort()){ // the current broker is responsible for this publisher
            int publisher = consumersToPublishers.get(port);

            try{
                //inform consumer that you are about to send him the video
                ObjectOutputStream cli_out = new ObjectOutputStream(clientCon.getOutputStream());
                cli_out.writeObject("ACCEPT");
                cli_out.flush();

                Socket pubConnx = new Socket(publisher, Publisher.getPort());

                //send request for music file to publisher
                ObjectOutputStream pub_out = new ObjectOutputStream(pubConnx.getOutputStream());
                pub_out.writeObject(new Pair<String, int>(title, port));
                pub_out.flush();

                // get files from publisher
                ObjectInputStream pubIn = new ObjectInputStream(pubConnx.getInputStream());
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
                    } catch (EOFException e) {
                        ++counter;
                    }
                }

                disconnect(pubConnx);
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

                clientOut.writeObject(consumersToBrokers);
                clientOut.flush();
            } catch (IOException e) {
                Extras.printError("BROKER: PULL: Could not use streams");
            }
            disconnect(clientCon);
        }
    }

    public void filterConsumers(){}
    public void updateNodes(){}

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

    
    public String toString(){
        return "Broker@"+getIP()+"@"+getPubPort()+"@"+getConPort()+"@"+getValue();
    }
}
