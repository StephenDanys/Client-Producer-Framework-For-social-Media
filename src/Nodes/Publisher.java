package Nodes;

import Extras.*;
import VideoFile.VideoFile;
import VideoFile.VideoFileHandler;
import channelName.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Publisher {
    private final int port; // port that Publisher uses as a server. It's also used as an Identifier
    private static final  String IP= "127.0.0.1";
    private final String RANGE; //range of artists (regex expression)
    private ArrayList<Pair<Integer, BigInteger>>  Brokers; //List of active Brokers( Port + HashValue)
    private Map<String, ArrayList<VideoFile>> files; //Map of Topics + videos that have this topic.
    private ServerSocket server;
    private ChannelName channelName;

    private Map<Integer, List<String>> brokersMap; //brokers (ports) and all their assigned hashtags
    private final ExecutorService threadPool;

    //constructor
    public Publisher(int port, String range, ChannelName name){
        this.port=port;
        RANGE = range;
        Extras.print("PUBLISHER: Construct publisher");
        this.channelName= name;
        threadPool = Executors.newCachedThreadPool();
    }

    /**
     * adds a record to files Map.
     * @param topic is key,
     * @param vids the videos associated with this key
     */
    public void  addHashTag(String topic, ArrayList<VideoFile> vids){
        files.put(topic, new ArrayList<VideoFile>());
        for (VideoFile vid : vids){
            vid.addAssociatedHashtag(topic);
            vid.setChannelName(channelName.getChannelName());
            files.get(topic).add(vid);
        }
    }

    /**
     * Remove one topic from files list
     * @param tag
     */
    public void  removeHashTag(String tag){
        files.remove(tag);
    }

    /**
     * Runs a
     * @param topic trough SHA1
     * @return BigInteger
     */
    public BigInteger hashTopic(String topic){
        return Extras.SHA1(topic);
    }

    /**
     * Find the videos you want through the topic
     * Break the video file into chunks and send them to broker
     * If a problem occurs (ex. song doesn't exist notify about failure via sending null
     * @param topic  video topic
     * @param connection open connection with broker
     */
    public void push(String topic, Socket connection){
        Extras.print("PUBLISHER: Push song to broker");

        //if artist doesn't exist notify about failure
        if (!files.containsKey(topic)){
            Extras.printError("PUBLISHER: ERROR: No such topic exists: "+topic);
            notifyFailure(connection);
            return;
        }
        //boolean found = false;
        ArrayList<VideoFile> chunks = new ArrayList();

        for(VideoFile video: files.get(topic)){ //for each video with this topic
            chunks.addAll(generateChunks(video));
            Extras.print(String.valueOf(chunks.size()));
            chunks.add(null);
        }
        chunks.add(null);
        if (chunks == null) {
            Extras.printError("PUBLISHER: ERROR: Video could not be broken to chunks");
            notifyFailure(connection);
            return;
        }
        //send to broker
        try {
            ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());

            for (VideoFile chunk : chunks){
                out.writeObject(chunk);
                out.flush();
            }

            chunks.clear(); //clear chunk list
        } catch (IOException e) {
            Extras.printError("PUBLISHER: ERROR: PUSH: Could not send file chunks");
            chunks.clear(); //clear chunk list
        }
    }

    /**
     * Make publisher online (await incoming connections)
     * Get the song, search for it and push it to broker
     */
    public void connect() {
        Extras.print("PUBLISHER: Going online");

        //open server socket
        try {
            //open server socket
            server = new ServerSocket(port);
        } catch (IOException e) {
            Extras.printError("PUBLISHER: ERROR: Server could not go online");
            try {
                if (server != null) server.close();
            } catch (IOException ex) {
                Extras.printError("PUBLISHER: ERROR: Server could not shut down");
            }
        }

        //create a thread, to await connection from brokers
        Thread task= new Thread(new Runnable() {
            @Override
            public void run() {
                Socket connection;
                try {
                    connection=server.accept();

                    //for each accepted connection create new thread
                    Thread processTask = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
                                String topic= (String) in.readObject();

                                //send all videos mapped to this topic
                                push(topic, connection);
                            } catch (IOException | ClassNotFoundException e) {
                                Extras.printError("PUBLISHER: ONLINE: ERROR: Could not read from stream");
                            }
                        }
                    });
                    threadPool.execute(processTask);
                } catch (IOException e) {
                    Extras.printError("PUBLISHER: ONLINE: ERROR: Could not accept connection");
                }
            }
        });
        threadPool.execute(task);
    }

    /**
     * Uses video FileHandler to split video in chuncks
     * @param video that we want to slpit
     * @return an arrayList with it's chunks
     */
    public ArrayList<VideoFile> generateChunks(VideoFile video){
        return VideoFileHandler.split(video);
    }
    /**
     * Initialize publisher
     * load videos, get all brokers, find with whom to connect and send them the creators
     * @param brokerPorts list with the ports assinged to brokers (comes from main method)
     */
    public boolean init(ArrayList<Integer> brokerPorts) {
        Extras.print("PUBLISHER: Initialize publisher");
        //load videos from file
        files= new HashMap<>();
        ArrayList<VideoFile> vfiles;
        vfiles = VideoFileHandler.readVideos(RANGE);
        if (vfiles == null || vfiles.isEmpty()) {
            Extras.printError("PUBLISHER: ERROR: No available songs");
            return false;
        }
        //initialize files, using channelName topic
        addHashTag(channelName.getChannelName(), vfiles);

        //here we can add more topics, while choosing videos that this topic will apply to
        //for the purposes of the 1st assignment, we'll only upload one video, with the channelName Topic

        //load every HashTag(String) from files into channelName.hashTagsPublished
        for(String topic : files.keySet()){
            channelName.addPublishedHashTag(topic);
        }
        Extras.print(files.toString()+"_____________");
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

        //connect with responsible broker and send them publisher's artists
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
     * Connect with broker, send him my port number and get its hash value
     * @param serverPort broker's port number
     */
    private Thread getServerHash(int serverPort) {
        Extras.print("PUBLISHER: Get server hash value");

        Thread thread = new Thread(new Runnable(){
            @Override
            public void run(){
                Socket connection;
                ObjectInputStream in;
                ObjectOutputStream out;
                BigInteger hashValue;
                try{
                    connection = new Socket("127.0.0.1", serverPort);
                    out = new ObjectOutputStream(connection.getOutputStream());
                    out.writeObject(port); //sending to broker my port so that he knows who i am
                    out.flush();
                    //get hash code
                    in = new ObjectInputStream(connection.getInputStream());
                    hashValue = (BigInteger) in.readObject();
                    updateBrokerList(serverPort, hashValue);
                } catch(IOException  e) {
                    Extras.printError("PUBLISHER: ERROR: Could not get hash of server " + serverPort);
                } catch (ClassNotFoundException c){
                    Extras.printError("here");
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
        Extras.print("PUBLISHER: Inform brokers for their Hashtags");

        for (int broker : brokersMap.keySet()) {
            Thread task = new Thread(new Runnable() {
                @Override
                public void run() {
                    Socket socket_conn = null;
                    try {
                        socket_conn = new Socket(IP, broker);

                        ObjectOutputStream out = new ObjectOutputStream(socket_conn.getOutputStream());
                        out.writeObject(port); //sending to broker my port so that he knows who i am
                        out.flush();


                        out.writeObject(brokersMap.get(broker));
                        out.flush();

                        //send to responsible brokers their hashtags
                        out.writeObject(null);
                        out.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                        Extras.printError("PUBLISHER: ERROR: Could not communicate topics to broker");
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
        Extras.print("PUBLISHER: Assign topics to responsible brokers");

        brokersMap = new HashMap<>();
        //find broker whose hash value is greater than the others
        Pair<Integer, BigInteger> maxBroker = brokerList.get(brokerList.size() - 1);
        BigInteger maxBrokerHash = maxBroker.getValue();

        for (String  hashTag: channelName.getHashTagsPublished()) {
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
