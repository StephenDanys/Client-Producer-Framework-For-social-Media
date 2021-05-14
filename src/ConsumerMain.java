import Extras.Pair;
import Nodes.Broker;

import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Console;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.Consumer;

public class ConsummerMain {
    private static BufferedReader reader;
    private static int input;

    private static Pair<String, BigInteger> credentials;
    private static Pair<String, Integer> extras;

    public static void main(String[] args){
        String IP;
        try{
            IP = InetAddress.getLocalHost().getHostAddress();
        }catch (UnknownHostException e){
            System.err.println("ERROR: Consumer down.");
            System.err.println("ERROR: Could not get IP address.");
            return;
        }
        if (IP.equals("127.0.0.1")){
            System.err.println("ERROR: Consumer down.");
            System.err.println("ERROR: IP is looback address.");
            return;
        }

        reader = new BufferedReader(new InputStreamReader(System.in));

        String serverIP;
        if(args.length<1){
            System.out.println("Server IP: ");
            serverIP = input();
        }else{
            serverIP = args[0];
        }

        Consumer consumer = new Consumer(IP,serverIP, Broker.getToCliPort());

        while(true) {
            if (!consumer.isLoggedin()){
                input = menu(0);
                switch (input){
                    //SIGN IN USER
                    case 1:
                        credentials = getCredentials();
                        extras = getExtras();
                        consumer.register(credentials);
                        break;
                    //LOGIN USER
                    case 2:
                        credentials = getCredentials();
                        consumer.loginUser(credentials);
                        break;
                    //EXIT
                    case 0:
                        try{
                            reader.close();
                        }catch (IOException e){
                            System.err.println("ERROR: Could not close buffer.")
                        }
                        System.exit(0);
                        //WRONG INPUT
                    default:
                        System.out.println("Wrong input.Try again.");
                }
            }
            //USER IS LOGGED IN
            if (consumer.isLoggedin()){
                input = menu(1);

            }//if
        }
    }//main






























































}//consummermain

