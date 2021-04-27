import java.util.*;

public interface Node {
    public List<Broker> Brokers=null;

    public void init(int x);
    public List<Broker> getBrokers();
    public void connect();
    public void disconnect();
    public void updateNodes();
}
