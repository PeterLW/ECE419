package common.cache;
public class Node{
    String key;
    String value;
    Node pre;
    Node next;

    public Node(String key, String value){
        this.key = key;
        this.value = value;
    }
}