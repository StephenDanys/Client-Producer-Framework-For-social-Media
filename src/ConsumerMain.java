
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.InetAddress;
import java.net.UnknownHostException;
import Nodes.Consumer;

public class ConsumerMain {
    private static BufferedReader reader;
    private static int input;

    public static void main(String[] args){
        String IP;
        try {
            IP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.err.println("ERROR: Consumer down");
            System.err.println("ERROR: Could not get IP address");
            return;
        }

        if (!IP.equals("127.0.0.1")) {
            System.err.println("ERROR: Consumer down");
            System.err.println("ERROR: IP is loopback address");
            return;
        }

        reader = new BufferedReader(new InputStreamReader(System.in));
        int assignedBroker=101;
        String serverIP;
        if(args.length < 1){
            System.out.println("Server IP: ");
            serverIP = input();
        }else{
            serverIP = args[0];
        }
        Consumer consumer;
        consumer= new Consumer(101);
        while(true){
            int input = menu();
            switch(input){
                case(1):
                    System.out.println("Please enter the topic you are interested on: ");
                    String topic = input();
                    System.out.println("Type mode1 to save chunks or mode2 to merge and save the video(mode2)");
                    String mode = input();
                    consumer.playData(topic,mode);
                //case(2):
                case(0):
                    System.out.println("Thank you!");
                    System.exit(0);
                    break;

            }
        }
    }

    /**
     * Print activity menus
     * @return user's choice
     */
    private static int menu() {
        System.out.println("---------- MENU ----------");
        System.out.println("1\tSearch for a song's topic");
        System.out.println("2\tEmpty for now");
        System.out.println("0\tExit");
        System.out.println("--------------------------");

        System.out.print("Enter: ");
        String userInput = input();
        return userInput != null ? Integer.parseInt(userInput) : 0;
    }

    /**
     * @return user input
     */
    private static String input() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            System.err.println("ERROR: Could not read input from user");
            return null;
        }
    }
}//main
