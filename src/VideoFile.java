import java.awt.*;
import java.util.*;

public class VideoFile {
    private String videoName;
    private String channelName;
    private String dateCreated;
    private String length;
    private String framerate;
    private String frameWidth;
    private String frameHeight;
    private ArrayList<String> associatedHashtags;
    private byte[] videoFileChunk;



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

    public String getFramerate(){
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

    public void setFramerate(String framerate){
        this.framerate = framerate;
    }

    public void setFrameWidth(String frameWidth){
        this.frameWidth = frameWidth;
    }

    public void setFrameHeight(String frameHeight){
        this.frameHeight = frameHeight;
    }

    public void setAssociatedHashtags(ArrayList<String> associatedHashtags){
        this.associatedHashtags = associatedHashtags;
    }

    public void setVideoFileChunk(byte[] videoFileChunk){
        this.videoFileChunk = videoFileChunk;
    }
}
