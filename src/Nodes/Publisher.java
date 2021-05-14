package Nodes;

import Extras.*;
import Nodes.Broker;
import VideoFile.Value;
import VideoFile.VideoFileHandler;
import channelName.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Publisher {
    private  final int port;
    private static final  String IP= "127.0. 0.1";
    private final String RANGE; //range of artists (regex expression)
    private ArrayList<Pair<Integer, BigInteger>>  Brokers; //List of active Brokers( Port + HashValue)
    private Map<String, ArrayList<Value>> files;//pairs of Hashtags + videos. Currently implemented as lots same videos
    private ServerSocket server;
    private ChannelName channelName;

    private Map<Integer, ArrayList<String>> brokersMap; //brokers (ports) and their individual Hashtags
    private final ExecutorService threadPool;

    //constructor
    public Publisher(int port, String range, ChannelName name){
        this.port=port;
        RANGE = range;
        Extras.print("PUBLISHER: Initialize publisher");
        this.channelName= name;
        threadPool = Executors.newCachedThreadPool();
    }

    //methods once online
    public void  addHashTag(String tag){

    }
    public void  removeHashTag(String tag){
    }

    /**
     * Runs a
     * @param topic trough SHA1
     * @return BigInteger
     */
    public BigInteger hashTopic(String topic){
        return Extras.SHA1(topic);
    }
    public void push(String topic, Value value){

    }
    /**
     * When a file with specific metadata doesn't exist send null
     * @param connection open connection with broker
     */
    private void notifyFailure(Socket connection) {
        Extras.print("PUBLISHER: Notify that song doesn't exist");

        ObjectOutputStream out;
        try {
            out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject(null);
            out.flush();
            out.writeObject(null);
            out.flush();
        } catch (IOException e) {
            Extras.printError("PUBLISHER: ERROR: PUSH: Could not send file chunks");
        }
    }

    public void notifyBrokersForHashTags(String Tag){

    }
    public ArrayList<Value> generateChunks(String v){

    }
    /**
     * Initialize publisher
     * load videos, get all brokers, find with whom to connect and send them the creators
     * @param brokerPorts list with the ports assinged to brokers (comes from main method)
     */
    public boolean init(ArrayList<Integer> brokerPorts) {
        Extras.print("PUBLISHER: Initialize publisher");
        //load videos from file
        files = VideoFileHandler.read(RANGE);
        if (files == null || files.isEmpty()) {
            Extras.printError("PUBLISHER: ERROR: No available songs");
            return false;
        }
        //load every HashTag(String) from files into channelName.hashTagsPublished
        for(String topic : files.keySet()){
            channelName.addHashTag(topic);
        }
        //get all active brokers
        getBrokers(brokerPorts);
        //find the brokers that are responsible for this publisher
        if (Brokers != null){
            if (Brokers.isEmpty()) {
                Extras.printError("PUBLISHER: ERROR: No brokers found for this publisher");
                return false;
            }
            //specify which broker is responsible for what Hashtag and the channelName, store results in brokerMap
            assignPublisherToBroker(Brokers);
        } else {
            Extras.printError("PUBLISHER: ERROR: No brokers initialized");
            return false;
        }

        //connect with responsible brokers
        //and send them publisher's artists
        informBrokers();
        return true;
    }
    /**
     * Get all brokers and sort them according to their ports
     * @param serverPorts brokers' ports
     */
    public void getBrokers(ArrayList<Integer> serverPorts){
        Extras.print("PUBLISHER: Getting Brokers");
        Brokers= new ArrayList<>();

        ArrayList<Thread> threads = new ArrayList<>();

        for (int port: serverPorts) {
            threads.add(getServerHash(port));
        }
        //before you continue wait for all threads to end
        for (Thread t : threads) {
            try {
                t.join();
            } catch(InterruptedException e) {
                Extras.printError("PUBLISHER: ERROR: Thread Interrupted");
            }
        }
        Brokers.sort(new Comparator<Pair<Integer, BigInteger>>() {
            @Override
            public int compare(Pair<Integer, BigInteger> a, Pair<Integer, BigInteger> b) {
                return a.getValue().compareTo(b.getValue());
            }
        });
    }
    /**
     * Connect with broker and get its hash value
     * @param serverPort broker's port number
     */
    private Thread getServerHash(int serverPort) {
        Extras.print("PUBLISHER: Get server hash value");

        Thread thread = new Thread(new Runnable(){
            @Override
            public void run(){
                Socket connection;
                ObjectInputStream in;
                BigInteger hashValue;
                try{
                    connection = new Socket(InetAddress.getByName(IP), serverPort);

                    //get hash code
                    in = new ObjectInputStream(connection.getInputStream());
                    hashValue = (BigInteger) in.readObject();
                    updateBrokerList(serverPort, hashValue);
                } catch(IOException | ClassNotFoundException e) {
                    Extras.printError("PUBLISHER: ERROR: Could not get hash of server " + serverPort);
                }
            }
        });
        thread.start();
        return thread;
    }
    /**
     * Add new Broker to brokerList
     * @param Port broker's Port number
     * @param HASH broker's hash value
     */
    private synchronized void updateBrokerList(int Port, BigInteger HASH){
        Brokers.add(new Pair<>(Port, HASH));
    }
    /**
     * Connect with responsible brokers and send them publisher's videos
     */
    private void informBrokers() {
        Extras.print("PUBLISHER: Inform brokers for their artists");

        for (int broker : brokersMap.keySet()) {
            Thread task = new Thread(new Runnable() {
                @Override
                public void run() {
                    Socket socket_conn = null;
                    try {
                        socket_conn = new Socket(IP, broker);

                        ObjectOutputStream out = new ObjectOutputStream(socket_conn.getOutputStream());
                        //send to responsible brokers their artists
                        out.writeObject(brokersMap.get(broker));
                        out.flush();
                    } catch (IOException e) {
                        Extras.printError("PUBLISHER: ERROR: Could not communicate artists to broker");
                    } finally {
                        disconnect(socket_conn);
                    }
                }
            });
            threadPool.execute(task);
        }
    }
    /**
     * Find the brokers that are responsible for this publisher's Hashtags
     * hash(Hashtag ) < hash(broker_IP + broker_port)
     * @param brokerList list with active brokers
     */
    private void assignPublisherToBroker (ArrayList<Pair<Integer, BigInteger>> brokerList ){
        Extras.print("PUBLISHER: Assign artists to responsible brokers");

        brokersMap = new HashMap<>();
        //find broker whose hash value is greater than the others
        Pair<Integer, BigInteger> maxBroker = brokerList.get(brokerList.size() - 1);
        BigInteger maxBrokerHash = maxBroker.getValue();

        for (String  hashTag: channelName.getHastagsPublished()) {
            //if hash(hashTag) > maximum hash(broker)
            //modulo with the maximum broker so that hash(hashTag) is in range [min_broker, max_broker]
            BigInteger hashValue= hashTopic(hashTag).mod(maxBrokerHash);

            for (Pair<Integer, BigInteger> broker : brokerList) { //for each broker port
                if (hashValue.compareTo(broker.getValue()) < 0) {
                    if (!brokersMap.containsKey(broker.getKey())) {
                        brokersMap.put(broker.getKey(), new ArrayList<String>());
                        //puts in the broker port
                    }
                    brokersMap.get(broker.getKey()).add(hashTag);
                    break;
                }
            }
        }
        BigInteger hashValue = hashTopic(channelName.getChannelName()).mod(maxBrokerHash);
        for (Pair<Integer, BigInteger> broker : brokerList) { //for each broker port
            if (hashValue.compareTo(broker.getValue()) < 0) {
                if (!brokersMap.containsKey(broker.getKey())) {
                    brokersMap.put(broker.getKey(), new ArrayList<String>());
                    //puts in the broker port
                }
                brokersMap.get(broker.getKey()).add(channelName.getChannelName());
                break;
            }
        }
    }

    public void connect(){}

    public void disconnect(Socket socket){
        Extras.print("PUBLISHER: Close socket connection");

        if (socket != null){
            try {
                socket.close();
            } catch (IOException e) {
                Extras.printError("PUBLISHER: ERROR: Could not close socket connection");
            }
        }
    }
}
