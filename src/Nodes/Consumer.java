package Nodes;

import Extras.*;
import VideoFile.*;

import java.math.BigInteger;
import java.util.*;
import java.lang.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.*;

public class Consumer {

    private final int PORT; //in this port will the consumer contact brokers
    private final String IP = "127.0.0.1";
    private HashMap<String, Integer> hashTagToBrokers = null; //hashtags assigned to brokers (Ports)
    private HashMap<String,Integer> publisherToBrokers =new HashMap<>(); //a publisher name targeted version of the above
    private List preview_videos;
    private Pair<String, BigInteger> user_credentials = null;
    private String STATE;
    private static String OUT = "LOGGED OUT";
    private static String IN = "LOGGED IN";

    public Consumer(int PORT ) {
        Extras.print("CONSUMER:  Create consumer");
        this.PORT = PORT;
        STATE = OUT;
        //shared_chunks = new ArrayList<>();
    }

    /**
     *
     * @param credentials
     * @param extra email and age
     * @return
     */
    public int register(Pair<String, BigInteger> credentials, Pair<String, Integer> extra) {
        Extras.print("CONSUMER: Register User");

        Socket connection = null;
        try {
            //OPEN CONNECTION
            connection = new Socket(IP, PORT);

            ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject("REGISTER");
            out.flush();

            //SEND CREDENTIALS TO BROKER
            out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject(credentials);
            out.flush();

            out.writeObject(extra);
            out.flush();

            //WAIT FOR CONFIRMATION
            ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
            String message = (String) in.readObject();
            if (message.equals("EXISTS_U")) {
                //USERNAME ALREADY EXISTS RETURN-1
                Extras.printError("CONSUMER: REGISTER: This username already exists.Choose another.");
                disconnect(connection);
                return -1;
            } else if (message.equals("EXISTS_E")) {
                //EMAIL ALREADY EXISTS RETURN -2
                Extras.printError("CONSUMER: REGISTER: This email already exists.Use another.");
                disconnect(connection);
                return -2;
            } else if (message.equals("TRUE")) {
                //SUCCESSFUL REGISTRATION RETURN 1
                STATE = IN;
                this.user_credentials = credentials;
                disconnect(connection);
                return 1;
            } else if (message.equals("FALSE")) {
                //UNSUCCESSFUL REGISTRATION RETURN 0
                Extras.printError("CONSUMER: REGISTER: ERROR: Could not register.Try again.");
                disconnect(connection);
                return 0;
            }

        } catch (IOException e) {
            Extras.printError("CONSUMER: REGISTER: ERROR: Could not get strams.");
        } catch (ClassNotFoundException e) {
            Extras.printError("CONSUMER: REGISTER: ERROR: Could not cast object to boolean.");
        }
        disconnect(connection);
        return -1;

    }

    public int loginUser(Pair<String, BigInteger> credentials) {
        Extras.print("CONSUMER: Log in user");

        Socket connection = null;
        try {
            connection = new Socket(IP, PORT);
            while(true){
                //GET CREDENTIALS FROM USER SEND THE TO BROKER
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                out.writeObject("LOGIN");
                out.flush();

                out = new ObjectOutputStream(connection.getOutputStream());
                out.writeObject(credentials);
                out.flush();

                //WAIT FOR CONFIRMATION
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
                String message = (String) in.readObject();
                switch (message){
                    //UREGISTERED USER
                    case "REGISTER":
                        disconnect(connection);
                        disconnect(connection);
                        return 0;
                    //REGISTERED USER
                    case "VERIFIED":
                        STATE = IN;
                        this.user_credentials = credentials;
                        disconnect(connection);
                        return 1;
                    //WRONG CREDENTIALS
                    case "FALSE":
                        Extras.printError("CONSUMER: LOGIN: ERROR: Could not login try again");
                        disconnect(connection);
                        return -1;
                }
            }
        } catch(IOException e){
            Extras.printError("CONSUMER: LOGIN: ERROR: Could not get streams");
        }catch(ClassNotFoundException e){
            Extras.printError("CONSUMER: LOGIN: ERROR: Could not cast Object to String");
        }
        // LOGIN FAILED
        disconnect(connection);
        return -2;
    }

    public void loggedout() {
        Extras.print("CONSUMER: Log out User.");
        STATE = OUT;
        this.user_credentials = null;
    }
    //RETURN TRUE IF USER IS LOGGED IN
    public boolean isLoggedin(){
        return STATE.equals(IN);
    }

    /**
     * Request video from broker
     * It's basicaly pull
     * If the Video is in the assigned broker(port) it is recieved
     * else broker sends a list of other brokers that we need.
     * This list is hashTagToBroker
     * @param topic channelName or hashtag
     * @mode whether to save chunks(mode1) or merge and save the video(save)
     */
    public ArrayList<VideoFile> playData(String topic, String title, String mode) {
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
                out.writeObject(title);
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
                        if(mode.equals("save")){
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
            Extras.printError("CONSUMER: PLAY: ERROR: Could not get streams.");
        } catch (ClassNotFoundException cnf){
            Extras.printError("CONSUMER: PLAY:  ERROR: Could not cast Object to String");
        }
        return videos;
    }

    /**
     * Get file chunks from stream
     * If mode1 is chosen then save each chunk(streaming)
     * If save is chosen the merge the chunks and save the video file
     * @param in   input stream
     */
    private ArrayList<VideoFile> receiveData(ObjectInputStream in, String mode) {
        ArrayList<VideoFile> returned = new ArrayList<>();
        ArrayList<VideoFile> chunks = new ArrayList<>();

        //resetChunks();

        VideoFile file; //COUNTER IS HOW MANY NULL STRINGS ARE READ
        int counter = 0; //WHEN COUNTER ==2 THEN END ALL FILE CHUNKS
        try {
            while (counter < 2) {
                try {
                    while ((file = (VideoFile) in.readObject()) != null) {
                        chunks.add(file);
                        /*if (mode.equals("mode1")) {
                            //addChunk(file);
                        }*/
                        counter = 0;
                    }
                    if (!chunks.isEmpty()) {
                        if (mode.equals("save")) {
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

    /**
     *Init of a client
     * loads the registered publisher of the
     * @param broker
     * that is assigned, in order to be later displayed in home screen
     */
    public List<VideoFile> loadChannelNames(int broker){
        //open connection
        Socket connection = null;
        try {
            connection = new Socket(IP, broker);

            //request brokers list
            ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject("INIT");
            out.flush();

            //get answer from broker
            ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
            String message = (String) in.readObject();
            //broker will send
            if(message.equals("FAILURE")){
                Extras.printError("CONSUMER: tried to Init without publishers");
            }else if(message.equals("ACCEPT")){
                ArrayList< String> list = (ArrayList<String>) in.readObject();
                for( String s : list){
                    publisherToBrokers.put(s,broker);
                }
                getBrokers(in);
            }
        } catch(IOException e){
            Extras.printError("CONSUMER: LOAD: ERROR: Could not get streams");
        } catch (ClassNotFoundException e){
            Extras.printError("CONSUMER: LOAD: ERROR: Could not cast Object to String");
        }

        disconnect(connection);
        return null;
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