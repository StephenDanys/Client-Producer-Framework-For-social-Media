package fileHandler;

import Extras.Extras;

import java.io.*;
import java.math.BigInteger;
import java.util.HashMap;

public class FileHandler {

    /*Creating a file for user credentials.
    * @return a file for user credentials*/

    public static File createUserFile(){

        //creating the directory for user credentials
        File dir = new File("./res/data/");

        if(!dir.exists()){
            if(!dir.mkdir()){
                Extras.printError("FILEHANDLER: ERROR: Could not create directory");
                return null;
            }
        }

        // creating the file for the users.
        File userFile = new File(dir, "users.txt");
        try {
            if (userFile.createNewFile()) {
                writeToUserFile("/* User Credentials */", userFile);
            }
        } catch (IOException e) {
            Extras.printError("FILEHANDLER: ERROR: Could not create user file");
            return null;
        }

        return userFile;
    }

    //append to the user file
    //@return true if the method is successful
    private static synchronized boolean writeToUserFile(String str, File file){
        try {
            file.setWritable(true);

            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
            writer.write(str);
            writer.newLine();

            writer.close();
            file.setWritable(false);

            return true;
        } catch(IOException e) {
            file.setWritable(false);
            Extras.printError("FILEHANDLER: ERROR: Could not write to user file");
            return false;
        }
    }

}
