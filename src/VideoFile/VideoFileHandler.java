package VideoFile;
import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;


public class VideoFileHandler {
    //it's an assumption that each hashtag is different, and some of them belong to a video
    public static Map<String, ArrayList<Value>> read(String range) {
        System.out.println("HANDLER: Reading music files");
        byte content[]=null;
        //get the directory
        File dir = new File("./res/dataset1/");
        if (!dir.exists()) {
            System.err.println("HANDLER: READ: ERROR: Directory doesn't exist");
            return null;
        }
        //get all files in the directory
        File[] files = dir.listFiles();

        Map<String, ArrayList<Value>> videos= new HashMap<>();
        if(files!=null){
            for(File file:files){
                try{
                    //meta data part
                    Metadata metadata = ImageMetadataReader.readMetadata(file);

                    for (Directory directory : metadata.getDirectories()) {
                        for (Tag tag : directory.getTags()) {
                            System.out.println(tag);
                        }
                    }
                    ArrayList<String> hashTags = new ArrayList<>();
                    content= Files.readAllBytes(file.toPath());
                    //if file doesn't mach range, continue

                    for (String hash : hashTags){
                        if(!videos.containsKey(hash)){
                            videos.put(hash, new ArrayList<Value>());
                        }
                    }

                } catch(IOException | ImageProcessingException ex) {
                    ex.printStackTrace();
                }
            }
        }
        return !videos.isEmpty() ? videos : null;
    }
}
