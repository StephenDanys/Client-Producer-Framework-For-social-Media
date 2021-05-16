package Nodes;
import Extras.Extras;
import Extras.Pair;
import VideoFile.*;

import java.util.*;

import java.lang.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.*;

public class Consumer {

    private final int PORT; //in this port will the consumer contact brokers
    private final String IP = "127.0.0.1";

    //private HashMap<String, BigInteger> channels = null; // PUBLISHER ASSIGNED TO BROKER
    private HashMap<String, Integer> hashTagToBrokers = null; //hashtags assigned to brokers (Ports)

    private ArrayList<VideoFile> preview_videos;
    private final ArrayList<VideoFile> shared_chunks;

    public Consumer(int PORT) {
        Extras.print("CONSUMER:  Create consumer");
        this.PORT = PORT;;
        shared_chunks = new ArrayList<>();
    }


    /**
     * Request video from broker
     * It's basicaly pull
     * If the Video is in the assigned broker(port) it is recieved
     * else broker sends a list of other brokers that we need.
     * This list is hashTagToBroker
     * @param topic channelName or hashtag
     * @mode whether to save chunks(mode1) or merge and save the video(mode2)
     */
    public ArrayList<VideoFile> playData(String topic, String mode) {
        Extras.print("CONSUMER: Video request");
        ArrayList<VideoFile> videos = null;

        //boolean state = false
        try {
            //CONSUMER HASN'T ASK FOR A VIDEO YET
            //ASK YOUR MAIN BROKER FOR A VIDEO
            if (hashTagToBrokers == null || hashTagToBrokers.isEmpty()) {
                //OPEN CONNECTION
                Socket connection = new Socket(IP, PORT);

                //REQUEST VIDEO
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                out.writeObject("PULL");
                out.flush();
                out.writeObject(topic);
                out.flush();

                //GET ANSWER FROM BROKER
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
                String message = (String) in.readObject();
                switch (message) {
                    //BROKER HAS THE VIDEO,SEND VIDEO
                    case "ACCEPT":
                        Extras.print("CONSUMER: Pull request accepted");
                        videos = receiveData(in, mode);
                        if(mode.equals("mode2")){
                            VideoFileHandler.write(videos);
                        }
                        disconnect(connection);
                        return videos;
                    //PUBLISHER DOESN'T EXIST
                    case "FAILURE":
                        Extras.printError("Publisher doesn't exist.");
                        disconnect(connection);
                        return null;
                    //BROKER DOESN'T HAVE THE VIDEO, SEND OTHER BROKERS
                    case "DECLINE":
                        getBrokers(in);
                }
                disconnect(connection);
            }
            //CHECK THE HASHMAP TO CHOOSE THE RIGHT BROKER
            int brokerPort = hashTagToBrokers.get(topic);
            if (brokerPort == 0) {
                Extras.printError("Publisher dosen't exist.");
                return null;
            }
            Socket connection = new Socket(IP, brokerPort);
            //REQUEST VIDEO
            ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject("PULL");
            out.flush();
            out.writeObject(topic);
            out.flush();

            //GET ANSWER FROM BROKER
            ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
            String message = (String) in.readObject();
            switch (message) {
                case "ACCEPT":
                    Extras.print("CONSUMER: Pull request accepted");
                    videos = receiveData(in, mode);
                    break;
                default:
                    Extras.printError("CONSUMER: PLAY: ERROR: INCONSISTENCY IN BROKERS.");
            }
            disconnect(connection);
        } catch (IOException e) {
            Extras.printError("CONSUMER: PLAY: ERROR: Could not grt strams.");
        } catch (ClassNotFoundException cnf){
            Extras.printError("CONSUMER: PLAY:  ERROR: Could not cast Object to String");
        }
        return videos;
    }

    public String getIP() {
        return IP;
    }

    /**
     * Get file chunks from stream
     * If mode1 is chosen then save each chunk(streaming)
     * If mode2 is chosen the merge the chunks and save the video file
     * @param in   input stream
     */
    private ArrayList<VideoFile> receiveData(ObjectInputStream in, String mode) {
        ArrayList<VideoFile> returned = new ArrayList<>();
        ArrayList<VideoFile> chunks = new ArrayList<>();

        resetChunks();

        VideoFile file; //COUNTER IS HOW MANY NULL STRINGS ARE READ
        int counter = 0; //WHEN COUNTER ==2 THEN END ALL FILE CHUNKS
        try {
            while (counter < 2) {
                try {
                    while ((file = (VideoFile) in.readObject()) != null) {
                        chunks.add(file);
                        if (mode.equals("mode1")) {
                            addChunk(file);
                        }
                        counter = 0;
                    }
                    if (!chunks.isEmpty()) {
                        if (mode.equals("mode2")) {
                            VideoFile merged = VideoFileHandler.merge(chunks);
                            returned.add(merged);
                        }
                    }
                    chunks.clear();
                    ++counter;
                } catch (IOException e) {
                   Extras.printError("CONSUMER: receiveData: exception in reading chunks");
                }
                if (counter > 2) break;
            }
            return returned;
        } catch (ClassNotFoundException e) {
            Extras.printError("CONSUMER: RECEIVE DATA: ERROR: Could not cast Object to MusicFile");
        }
        return null;
    }

    public int getChunkListSize(){
        synchronized (shared_chunks){
            return shared_chunks.size();
        }
    }

    public VideoFile getNextChunk(){
        synchronized (shared_chunks){
            return shared_chunks.remove(0);
        }
    }

    public VideoFile viewNextChunk(){
        synchronized (shared_chunks){
            return shared_chunks.get(0);
        }
    }

    private void resetChunks(){
        synchronized (shared_chunks){
            shared_chunks.clear();
        }
    }

    private void getBrokers(ObjectInputStream in){
        try{
            hashTagToBrokers = (HashMap) in.readObject();
        }catch(IOException e){
            Extras.printError("CONSUMER: BROKERS: ERROR: Could not get streams.");
        }catch (ClassNotFoundException e){
            Extras.printError("CONSUMER: BROKERS: ERROR: Could not cast Object to HashMap.");
        }
    }

    /**
     * Add chunk to shared_chunks arraylist
     *
     * @param chunk to be added
     */
    private void addChunk(VideoFile chunk) {
        synchronized (shared_chunks){
            shared_chunks.add(chunk);
        }
    }

    private void disconnect(Socket socket){
        Extras.printError("CONSUMER: Close socket connection.");
        if (socket!= null){
            try{
                socket.close();
            }catch (IOException e){
                Extras.printError("PUBLISHER: ERROR: Could not close socket connection.");
            }
        }

    }
}