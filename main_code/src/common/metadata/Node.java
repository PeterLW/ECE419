package common.metadata;

import java.math.BigInteger;

public class Node implements Comparable<Node>{
    String id;
    BigInteger hash;

    public Node(String id, BigInteger hash){
        this.id = id;
        this.hash = hash;
    }

    public Node(){
        this.id = id;
        this.hash = hash;
    }

    @Override
    public int compareTo(Node o) {
        return this.hash.compareTo(o.hash);
    }
}
