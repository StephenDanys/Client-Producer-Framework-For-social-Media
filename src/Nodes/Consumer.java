import Extras.Extras;
import Extras.Pair;
import VideoFile.VideoFile;

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

    private Pair<String, BigInteger> user_credentials = null;
    private String STATE;
    private static String OUT = "LOGGED OUT";
    private static String IN = "LOGGED IN";

    private HashMap<String, BigInteger> channels = null; // PUBLISHER ASSIGNED TO BROKER
    private HashMap<String, Integer> publishers = null; //artists assigned to brokers (IP addresses)

    private List<VideoFile> preview_videos;
    private final List<VideoFile> shared_chunks;

    //REGISTER USER TO BROKER
    public Consumer(int PORT) {
        Extras.print("CONSUMER:  Create consumer");
        this.PORT = PORT;
        STATE = OUT;
        shared_chunks = new ArrayList<>();
    }

    public int register(Pair<String, BigInteger> credentials, Pair<String, Integer> extra) {
        Extras.print("CONSUMER: Register User");

        Socket connection = null;
        try {
            //OPEN CONNECTION
            connection = new Socket(SERVER_IP, PORT);

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
                //SUCCEFUL REGISTRATION RETURN 1
                STATE = IN;
                this.user_credentials = credentials;
                disconnect(connection);
                return 1;
            } else if (message.equals("FALSE")) {
                //UNSUCCEFUL REGISTRATION RETURN 0
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
            connection = new Socket(SERVER_IP, PORT);
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
     * REQUEST VIDEO FROM MAIN BROKER
     * IF VIDEO IS IN MAIN BROKER, IT IS RECIEVED
     * ELSE BROKER SEND A LISTS OF OTHER BROKERS THAT WILL BE REQUIRED
     */
    public ArrayList<VideoFile> playData(String channelName, String videoN, String mode) {
        Extras.print("CONSUMER: Video request");
        ArrayList<VideoFile> videos = null;

        //boolean state = false
        try {
            //CONSUMER HASN'T ASK FOR A VIDEO YET
            //ASK YOUR MAIN BROKER FOR A VIDEO
            if (channels == null || channels.isEmpty()) {
                //OPEN CONNECTION
                Socket connection = new Socket(SERVER_IP, PORT);

                //REQUEST VIDEO
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                out.writeObject("PULL");
                out.flush();
                out.writeObject(new Pair<>(channelN, videoN));
                out.flush();

                //GET ANSWERFROM BROKER
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
                String message = (String) in.readObject();
                switch (message) {
                    //BROKER HAS THE VIDEO,SEND VIDEO
                    case "ACCEPT":
                        videos = recieveData(in, mode);
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
            //CHECK THE CHANNELS LIST TO CHOOSE THE RIGHT BROKER
            BigInteger brokerIP = channels.get(videoN);
            if (brokerIP == null) {
                Extras.printError("Publisher dosen't exist.");
                return null;
            }
            Socket connection = new Socket(SERVER_IP, PORT);
            //REQUEST VIDEO
            ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject("PULL");
            out.flush();
            out.writeObject(new Pair<>(channelN, videoN));
            out.flush();

            //GET ANSWER FROM BROKER
            ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
            String message = (String) in.readObject();
            switch (message) {
                case "ACCEPT":
                    videos = recieveData(in, mode);
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
     * If online mode is chosen then save each chunk
     * If offline mode is chosen the merge the chunks and save the music file
     *
     * @param in   input stream
     * @param mode online or offline
     */
    private ArrayList<VideoFile> recieveData(ObjectInputStream in, String mode) {
        ArrayList<VideoFile> returned = new ArrayList<>();
        ArrayList<VideoFile> chunks = new ArrayList<>();

        resetChunks();

        VideoFile file;
        int counter = 0; //WHEN COUNTER ==2 THEN END ALL FILE CHUNKS
        try {
            while (counter < 2) {
                try {
                    while ((file = (VideoFile) in.readObject()) != null) {
                        chunks.add(file);
                        if (mode.equals("ONLINE")) {
                            addChunk(file);
                        }
                        counter = 0;
                    }
                    if (!chunks.isEmpty()) {
                        if (mode.equals("OFFLINE")) {
                            VideoFile merged = VideoFileHandler.merge(chunks);
                            returned.add(merged);
                        } else if (mode.equals("INFO")) {
                            VideoFile preview = chunks.get(0);
                            preview.setVideoName(preview.getVideoName().substring(2));
                            preview.setChannelName(null);
                            preview.setDateCreated(null);
                            preview.setVideoFileChunk(null);
                            preview.setAssociatedHashtags(null);
                            preview_videos.add(preview);
                        }
                    }
                    chunks.clear();
                } catch (IOException e) {
                    ++counter;
                }
                if (counter > 2) break;
            }
            return returned;
        } catch (IOException e) {
            Extras.printError("CONSUMER: RECEIVE DATA: ERROR: Could not get streams");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            Extras.printError("CONSUMER: RECEIVE DATA: ERROR: Could not cast Object to MusicFile");
        }
        return null;
    }

    public List<VideoFile> loadChannels(){
        //open connection
        Socket connection = null;
        try {
            connection = new Socket(IP, PORT);

            //request brokers list
            ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject("INIT");
            out.flush();

            //get answer from broker
            ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
            String message = (String) in.readObject();
            //broker will send
            if(message.equals("DECLINE")){
                getBrokers(in);
            }
            disconnect(connection);
            preview_videos = new ArrayList<>();
            for(String channelName : channels.keySet()){
                playData("", "", "INFO");
            }
            return preview_videos;
        } catch(IOException e){
            Extras.printError("CONSUMER: LOAD: ERROR: Could not get streams");
        } catch (ClassNotFoundException e){
            Extras.printError("CONSUMER: LOAD: ERROR: Could not cast Object to String");
        }

        disconnect(connection);
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
            publishers = (HashMap) in.readObject();
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