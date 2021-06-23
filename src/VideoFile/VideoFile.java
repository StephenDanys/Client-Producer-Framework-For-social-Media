package VideoFile;
import java.awt.*;
import java.io.Serializable;
import java.util.*;

public class VideoFile implements Serializable {
    private String videoName;
    private String channelName;
    private String dateCreated;
    private String length;
    private String framerate;
    private String frameWidth;
    private String frameHeight;
    private ArrayList<String> associatedHashtags;
    private byte[] videoFileChunk;
    private int serial = 0; //serial number for chunks

    //constructor
    public  VideoFile(String videoName, String channelName, String dateCreated, String length, String framerate, String frameWidth, String frameHeight,
                      ArrayList<String> associatedHashtags, byte[] videoFileChunk){
        this.videoName=videoName;
        this.channelName=channelName;
        this.dateCreated=dateCreated;
        this.length=length;
        this.framerate=framerate;
        this.frameWidth=frameWidth;
        this.frameHeight=frameHeight;
        this.associatedHashtags=associatedHashtags;
        this.videoFileChunk=videoFileChunk;
    }
    public VideoFile(String videoName, String channelName, String dateCreated, String length, String framerate, String frameWidth, String frameHeight,
                     ArrayList<String> associatedHashtags, byte[] videoFileChunk, int serial){
        this(videoName,channelName,dateCreated,length,framerate,frameWidth,frameHeight,associatedHashtags,videoFileChunk);
        this.serial=serial;
    }


    //Accessors
    public String getVideoName(){
        return videoName;
    }

    public String getChannelName(){
        return channelName;
    }

    public String getDateCreated(){
        return dateCreated;
    }

    public String getLength(){
        return length;
    }

    public String getFrameRate(){
        return framerate;
    }

    public String getFrameWidth(){
        return frameWidth;
    }

    public String getFrameHeight(){
        return frameHeight;
    }

    public byte[] getVideoFileChunk(){
        return videoFileChunk;
    }

    public ArrayList<String> getAssociatedHashtags(){
        return associatedHashtags;
    }

    //Mutators
    public void setVideoName(String videoName){
        this.videoName = videoName;
    }

    public void setChannelName(String channelName){
        this.channelName = channelName;
    }

    public void setDateCreated(String dateCreated){
        this.dateCreated = dateCreated;
    }

    public void setLength(String length){
        this.length = length;
    }

    public void setFrameRate(String framerate){
        this.framerate = framerate;
    }

    public void setFrameWidth(String frameWidth){
        this.frameWidth = frameWidth;
    }

    public void setFrameHeight(String frameHeight){
        this.frameHeight = frameHeight;
    }

    public void addAssociatedHashtag(String associatedHashtag){
            associatedHashtags.add(associatedHashtag);

    }
    public void setAssociatedHashtags(ArrayList<String> list){
        associatedHashtags= list;
    }

    public void removeAssociatedHashtag(String topic){
        associatedHashtags.remove(topic);
    }

    public void setVideoFileChunk(byte[] videoFileChunk){
        this.videoFileChunk = videoFileChunk;
    }
    public int getSerial(){
        return serial;
    }
    public int compareTo(VideoFile b){
        return Integer.compare(this.getSerial(), b.getSerial());
    }
}
