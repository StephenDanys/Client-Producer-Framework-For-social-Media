package VideoFile;
import java.io.File;
import java.io.IOException;
import Extras.Extras;
import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.Arrays;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;



public class VideoFileHandler {

    public static VideoFile read(String path){
        System.out.println("HANDLER: Reading video");

        int numForSwitch = 1;
        String[] tableElements;
        String videoName = "";
        String dateCreated = "";
        String length = "";
        String frameRate = "";
        String frameWidth = "";
        String frameHeight = "";

        VideoFile viFile = null; //initializing the videoFile instance

        File singleVideo = new File(path); //creating the file object using the path parameter

        if(singleVideo!=null){
            try{
                //getting the metadata of that file
                Metadata metadata = ImageMetadataReader.readMetadata(singleVideo);

                for (Directory directory : metadata.getDirectories()) {
                    for (Tag tag : directory.getTags()) {
                        String stag = tag.toString();
                        switch (numForSwitch) {
                            case (4):
                                tableElements = stag.split("-");
                                dateCreated = tableElements[1];
                            case (6):
                                tableElements = stag.split("-");
                                length = tableElements[1];
                            case (20):
                                tableElements = stag.split("-");
                                frameWidth = tableElements[1];
                            case (21):
                                tableElements = stag.split("-");
                                frameHeight = tableElements[1];
                            case (25):
                                tableElements = stag.split("-");
                                frameRate = tableElements[1];
                            case (38):
                                tableElements = stag.split("-");
                                videoName = tableElements[1];


                        }
                        numForSwitch++;

                    }
                }

                ArrayList<String> associatedHashTags = new ArrayList<>();

                byte[] fileContent = Files.readAllBytes(singleVideo.toPath());

                viFile = new VideoFile(videoName,"",dateCreated,length,frameRate,frameWidth,frameHeight,associatedHashTags,fileContent);

            } catch(IOException | ImageProcessingException ex) {
                ex.printStackTrace();
            }
        }

        return singleVideo!=null ? viFile : null;
    }
    //it's an assumption that each hashtag is different, and some of them belong to a video
    public static ArrayList<VideoFile> readVideos(String range) {
        System.out.println("HANDLER: Reading video files");

        int numForSwitch = 1;
        String[] tableElements;
        String videoName = "";
        String dateCreated = "";
        String length = "";
        String frameRate = "";
        String frameWidth = "";
        String frameHeight = "";

        //get the directory
        File dir = new File("./dataset1/");

        if (!dir.exists()) {
            System.err.println("HANDLER: READ: ERROR: Directory doesn't exist");
            return null;
        }

        //get all files in the directory
        File[] files = dir.listFiles();

        ArrayList<VideoFile> fileChunks = new ArrayList<>();//the list with all the video chunks

        if(files!=null){

            for(File file:files){
                System.out.println(file.toString());
                try{
                    //getting the metadata of that file
                    Metadata metadata = ImageMetadataReader.readMetadata(file);

                    for (Directory directory : metadata.getDirectories()) {
                        for (Tag tag : directory.getTags()) {
                            String stag = tag.toString();
                            switch (numForSwitch) {
                                case (4):
                                    tableElements = stag.split("-");
                                    dateCreated = tableElements[1];
                                case (6):
                                    tableElements = stag.split("-");
                                    length = tableElements[1];
                                case (20):
                                    tableElements = stag.split("-");
                                    frameWidth = tableElements[1];
                                case (21):
                                    tableElements = stag.split("-");
                                    frameHeight = tableElements[1];
                                case (25):
                                    tableElements = stag.split("-");
                                    frameRate = tableElements[1];
                                case (38):
                                    tableElements = stag.split("-");
                                    videoName = tableElements[1];


                            }
                            numForSwitch++;

                        }
                    }

                    ArrayList<String> associatedHashTags = new ArrayList<>();

                    byte[] fileContent = Files.readAllBytes(file.toPath());

                    VideoFile viFile = new VideoFile(videoName,"",dateCreated,length,frameRate,frameWidth,frameHeight,associatedHashTags,fileContent);
                    fileChunks.add(viFile);


                } catch(IOException | ImageProcessingException ex) {
                    ex.printStackTrace();
                }
            }
        }
        return !fileChunks.isEmpty() ? fileChunks : null;
    }

    //save a song to Downloads directory
    public static boolean write(VideoFile file){
        Extras.print("HANDLER: Writing video file");

        //if the file is null then cancel the activity
        if (file == null) {
            Extras.printError("HANDLER: WRITE: ERROR: Null object passed");
            return false;
        }

        //if the metadata are not present
        if (file.getVideoName() == null || file.getChannelName() == null){
            Extras.printError("HANDLER: WRITE: ERROR: Null metadata in file");
            return false;
        }

        //create a directory if ti does not exist
        File dir = new File("././download/");
        /*if (!dir.exists()) {
            if (!dir.mkdir()) {
                Extras.printError("HANDLER: WRITE: ERROR: Could not create directory");
                return false;
            }
        }
        */
        try{
            FileOutputStream out = new FileOutputStream(dir + file.getVideoName() + ".mp4");
            out.write(file.getVideoFileChunk());

            return true;

        } catch (IOException e) {
            Extras.printError("HANDLER: WRITE: ERROR: Could not write file to directory");
            return false;
        }
    }

    //save video chunks to the stream directory
    public static boolean write(ArrayList<VideoFile> chunks){
        Extras.print("HANDLER: Writing chunks");

        //if the chunk list is null then cancel the activity
        if (chunks == null || chunks.isEmpty()) {
            Extras.printError("HANDLER: WRITE CHUNKS: ERROR: Null object passed");
            return false;
        }

        //create directory if it doesn't exist
        File dir = new File(".\\download\\");
        /*
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                Extras.printError("HANDLER: WRITE CHUNKS: ERROR: Could not create directory");
                return false;
            }
        }
        */

        for(VideoFile chunk : chunks){
            //create file
            try{
                FileOutputStream out = new FileOutputStream(dir + chunk.getVideoName() + ".mp4");
                out.write(chunk.getVideoFileChunk());

            } catch (IOException e) {
                Extras.printError("HANDLER: WRITE CHUNK: ERROR: Could not write file to directory");
                return false;
            }
        }
        return true;
    }

    //splits a file into 512Kb chunks
    public static ArrayList<VideoFile> split (VideoFile file){
        Extras.print("HANDLER: Splitting video file");

        //if the file is null then cancel the activity
        if (file == null) {
            Extras.printError("HANDLER: WRITE: ERROR: Null object passed");
            return null;
        }

        //the byte[] array of the videoFile we want to split
        byte[] videoBytes = file.getVideoFileChunk();

        //if file doesn't contain a byte array
        if ((videoBytes == null) || (videoBytes.length == 0)){
            Extras.printError("HANDLER: SPLIT: ERROR: File without byte array");
            return null;
        }

        int chunkSize = 256000; //256KB is the size we want to split our videos
        ArrayList<VideoFile> chunks = new ArrayList<>(); //the ArrayList which will contain all the video chunks we will have after the split

        int start = 0; //the pointer of the whole Array. This will help us splitting the table into smaller ones
        int serial = 1; //the serial number each of our VideoFile chunks will have

        //as long as we have not covered the entire array we create more byte[] chunks
        while(start < videoBytes.length){
            byte[] chunk = new byte[chunkSize]; //the byte array of each chunk
            chunk = Arrays.copyOfRange(videoBytes,start,start + chunkSize);
            start += chunkSize;
            VideoFile chunkV = new VideoFile(file.getVideoName(),file.getChannelName(),file.getDateCreated(),file.getLength(),file.getFrameRate(),file.getFrameWidth(),file.getFrameHeight(),file.getAssociatedHashtags(), file.getVideoFileChunk(),serial);
            chunks.add(chunkV); //adding the VideoFile chunk into the ArrayList
            serial++;
        }

        return chunks;

    }

    //merge the file chunks into one file
    public static VideoFile merge (ArrayList<VideoFile> chunks){
        Extras.print("HANDLER: Merging video file chunks");

        //if the file is null then cancel the activity
        if (chunks == null || chunks.size() == 0) {
            Extras.printError("HANDLER: MERGE: ERROR: No chunks where passed");
            return null;
        }

        /*
        //if list contains one chunk only
        if (chunks.size() == 1) {
            //write correct title
            chunks.get(0).setVideoName(chunks.get(0).getVideoName().substring(chunks.get(0).getVideoName().indexOf(" ") + 1));
            return chunks.get(0);
        }
        */
        //sort chunks according to serial number
        chunks.sort(new Comparator<VideoFile>() {
            public int compare(VideoFile a, VideoFile b) {
                return a.compareTo(b);
            }
        });

        //get bytes from chunks using a byte[] array
        byte[] newFile = new byte[(chunks.size() +2) * 256000];
        int start =0; //it is the main pointer in the array that you will help us with the copy

        chunks.sort(VideoFile::compareTo); /*we are sorting the videoFiles using their serial numbers, in order to place them in the
                                              right order for the final videoFile.*/

        //for each videoFile we copy its byte array to the main array and we add to the pointer
        for(VideoFile vf : chunks){
            System.arraycopy(vf.getVideoFileChunk(),0,newFile,start,vf.getVideoFileChunk().length);
            start += vf.getVideoFileChunk().length;
        }

        //creating the whole videoFile object
        VideoFile nFile = new VideoFile(chunks.get(0).getVideoName(),chunks.get(0).getChannelName(),chunks.get(0).getDateCreated(),chunks.get(0).getLength(),
                chunks.get(0).getFrameRate(),chunks.get(0).getFrameWidth(),chunks.get(0).getFrameHeight(),chunks.get(0).getAssociatedHashtags(),
                newFile);

        return nFile;
    }
}
