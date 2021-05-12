package channelName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ChannelName {
    private String channelName;
    private HashSet<String> hastagsPublished;
    private HashMap<String, ArrayList<Value>> userVideoFilesMap;

    public ChannelName(String channelName, HashSet<String> hastagsPublished, HashMap<String, ArrayList<Value>> userVideoFilesMap){
        this.channelName=channelName;
        this.hastagsPublished=hastagsPublished;
        this.userVideoFilesMap=userVideoFilesMap;
    }
    public String  getChanelName(){
        return channelName;
    }
    public HashSet<String> getHastagsPublished(){
        return hastagsPublished;
    }
    public HashMap<String, ArrayList<Value>> getUserVideoFilesMap(){
        return userVideoFilesMap;
    }

    public void addHashTag(String Hashtag){
        hastagsPublished.add(Hashtag);
    }
    public boolean removeHashTag(String Hashtag){
        for(String hash : hastagsPublished){
            if (hash.equals(Hashtag)){
                hastagsPublished.remove(hash);
                return true;
            }
        }
        return false;
    }
}
