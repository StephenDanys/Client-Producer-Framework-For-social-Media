package VideoFile;
import channelName.*;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class VideoFileHandler {

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
                    //meta data part goes here too

                    content= Files.readAllBytes(file.toPath());

                    //if file doesn't mach range, continue

                videos
                } catch(IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
