package common.cache;
import common.cache.Node;
import java.util.*;

public class LRU {
    int capacity;
    static HashMap<String, Node> map = new HashMap<String, Node>();
    Node head=null;
    Node end=null;

    public LRU(int capacity) {
        this.capacity = capacity;
    }

    public String getKV(String key) {
        if(map.containsKey(key)){
            Node n = map.get(key);
            remove(n);
            setHead(n);
            return n.value;
        }
        return null;
    }

    public void remove(Node n){
        if(n.pre!=null){
            n.pre.next = n.next;
        }else{
            head = n.next;
        }
        if(n.next!=null){
            n.next.pre = n.pre;
        }else{
            end = n.pre;
        }
    }

    public void setHead(Node n){
        n.next = head;
        n.pre = null;

        if(head!=null)
            head.pre = n;
        head = n;
        if(end ==null)
            end = head;
    }

    public void putKV(String key, String value) {
        if(map.containsKey(key)){
            Node old = map.get(key);
            old.value = value;
            remove(old);
            setHead(old);
        }else{
            Node created = new Node(key, value);
            if(map.size()>=capacity){
                map.remove(end.key);
                remove(end);
                setHead(created);

            }else{
                setHead(created);
            }

            map.put(key, created);
        }
    }

    public void clear(){
        map.clear();
    }

    public boolean in_LRU(String key){
        return map.containsKey(key);
    }

    public int get_cache_size(){
        return capacity;
    }
}