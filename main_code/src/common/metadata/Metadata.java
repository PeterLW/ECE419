package common.metadata;

import java.security.*;
import java.lang.*;
import java.math.BigInteger;
import java.util.*;

public class Metadata {
    // bst
    private static TreeSet<Node> servers_bst;
    private static HashMap<BigInteger, String> HashToServer;
    private Node first_node = null;
    private Node last_node = null;

    public Metadata(){
        servers_bst = new TreeSet<Node>();
        HashToServer = new HashMap<BigInteger, String>();
    }

    public Metadata(Node first_node, HashMap<BigInteger, String> HashToServer){
        this.first_node = first_node;
        this.HashToServer = HashToServer;
    }

    private void build_bst(){
        System.out.println("building bst...");
        remove_all();

        Iterator i = HashToServer.entrySet().iterator();

        while(i.hasNext()){
            Map.Entry pair = (Map.Entry)i.next();
            String server = (String)pair.getValue();
            System.out.println("build: " + server);
            add_server(server);
        }
    }

    public String find_server(BigInteger hash){
        Node temp = new Node("0.0.0.0", hash);

        if(hash.compareTo(last_node.hash) == 1)
            return first_node.address;
        else{
            return servers_bst.higher(temp).address;
        }
    }

    public void update_hash2server(){
        Iterator i = servers_bst.iterator();

        if(i.hasNext()){
            first_node = servers_bst.first();
            last_node = servers_bst.last();
        }

        while (i.hasNext()) {
            Node current_node = (Node) i.next();
            HashToServer.put(current_node.hash, current_node.address);
        }
    }

    public void remove_server(String server){
        try {
            Node s = new Node(server, getMD5(server));
            if(servers_bst.contains(s))
                servers_bst.remove(s);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void add_server(String server){
        try {
            Node s = new Node(server, getMD5(server));
            if(!servers_bst.contains(s))
                servers_bst.add(s);
            else
                System.err.println("Server already exist or Hash collosion!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void remove_all(){
        if(!servers_bst.isEmpty())
            servers_bst.clear();
    }

    public boolean is_empty(){
        if(!servers_bst.isEmpty())
            return false;
        else
            return true;
    }

    private BigInteger getMD5(String input) throws Exception{

        MessageDigest md=MessageDigest.getInstance("MD5");
        md.update(input.getBytes(),0,input.length());
        String hash_temp = new BigInteger(1,md.digest()).toString(16);
        BigInteger hash = new BigInteger(hash_temp, 16);
        return hash;
    }


    public void printdata(String s){
        if(s.equals("bst")) {
            Iterator i = servers_bst.iterator();
            System.out.println("Printing BST...");
            while (i.hasNext()) {
                Node temp = (Node) i.next();
                System.out.println(temp.hash + ": " + temp.address);
            }
            System.out.println("--------------------------------------------");
        }

        if(s.equals("h2s")) {
            System.out.println("Printing HashToServer...");
            Iterator i = HashToServer.entrySet().iterator();
            while (i.hasNext()) {
                Map.Entry pair = (Map.Entry) i.next();
                System.out.println(pair.getKey() + ":" + pair.getValue());
            }
            System.out.println("--------------------------------------------");
        }
    }



    public static void main(String[] args){

        Metadata m = new Metadata();

        //test for add/remove server and update hash2server
        m.add_server("a");
        m.add_server("b");
        m.add_server("c");
        m.add_server("d");
        m.add_server("e");
        m.add_server("f");
        m.remove_server("c");
        m.remove_server("f");
        m.update_hash2server();
        m.printdata("bst");
        m.printdata("h2s");


        //test for build bst from hash2server
        m.remove_all();
        if(m.is_empty())
            System.out.println("bst empty");
        m.build_bst();
        m.printdata("bst");


        //test for find server
        BigInteger test_val = new BigInteger("16955237001963240173058271559858726496");
        String server = m.find_server(test_val);
        System.out.println(server);
    }
}

