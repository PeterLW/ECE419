package common;

import java.math.BigInteger;

public class Node implements Comparable<Node>{
    String address;
    BigInteger hash;

    public Node(String address, BigInteger hash){
        this.address = address;
        this.hash = hash;
    }

    public Node(){
        this.address = address;
        this.hash = hash;
    }

    @Override
    public int compareTo(Node o) {
        return this.hash.compareTo(o.hash);
    }
}
