package Nodes;

import java.util.*;

public interface Node {
    public ArrayList<Broker> Brokers=null;

    public void init(int x);
    public List<Broker> getBrokers();
    public void connect();// go online
    public void disconnect(); //close/ stop connections
    public void updateNodes();
}
