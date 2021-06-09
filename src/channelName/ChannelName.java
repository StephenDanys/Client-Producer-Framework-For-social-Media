package channelName;

import java.util.HashSet;

public class ChannelName {
    private String channelName; //name of channel;
    private HashSet<String> hashTagsPublished;  //collection of hashTags that this channel is responsible for

    public ChannelName(String channelName, HashSet<String> hashTagsPublished){
        this.channelName=channelName;
        this.hashTagsPublished=hashTagsPublished;
    }
    public String  getChannelName(){
        return channelName;
    }
    public HashSet<String> getHashTagsPublished(){
        return hashTagsPublished;
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
