package Extras;

import java.io.Serializable;

public class Triplet<V1, V2, V3> implements Serializable {
    private V1 value1;
    private V2 value2;
    private V3 value3;

    public Triplet(V1 value1, V2 value2, V3 value3){
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
    }

    public V1 getValue1(){
        return value1;
    }

    public V2 getValue2(){
        return value2;
    }
}
