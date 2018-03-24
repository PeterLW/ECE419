package common.cache;
public class LRUNode {
    String key;
    String value;
    LRUNode pre;
    LRUNode next;

    public LRUNode(String key, String value){
        this.key = key;
        this.value = value;
    }
}