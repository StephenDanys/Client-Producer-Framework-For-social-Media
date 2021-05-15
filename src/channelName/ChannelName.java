package channelName;
import VideoFile.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ChannelName {
    private String channelName;
    private HashSet<String> hashTagsPublished;
    private HashMap<String, ArrayList<Value>> userVideoFilesMap;

    public ChannelName(String channelName, HashSet<String> hastagsPublished, HashMap<String, ArrayList<Value>> userVideoFilesMap){
        this.channelName=channelName;
        this.hashTagsPublished=hastagsPublished;
        this.userVideoFilesMap=userVideoFilesMap;
    }
    public String  getChannelName(){
        return channelName;
    }
    public HashSet<String> getHashTagsPublished(){
        return hashTagsPublished;
    }
    public HashMap<String, ArrayList<Value>> getUserVideoFilesMap(){
        return userVideoFilesMap;
    }

    public void addPublishedHashTag(String Hashtag){
        hashTagsPublished.add(Hashtag);
    }
    public boolean removeHashTag(String Hashtag){
        for(String hash : hashTagsPublished){
            if (hash.equals(Hashtag)){
                hashTagsPublished.remove(hash);
                return true;
            }
        }
        return false;
    }
}
