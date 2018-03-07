package common.metadata;

import java.security.*;
import java.lang.*;
import java.math.BigInteger;
import java.util.*;

public class Metadata {
    // bst
    private transient static TreeSet<Node> servers_bst;
    private static HashMap<BigInteger, String> HashToServer;
    private transient Node first_node = null;
    private transient Node last_node = null;

    public Metadata(){
        servers_bst = new TreeSet<Node>();
        HashToServer = new HashMap<BigInteger, String>();
    }

    public Metadata(Node first_node, HashMap<BigInteger, String> HashToServer){
        this.first_node = first_node;
        this.HashToServer = HashToServer;
    }

    //primarily for KVClient to search correct server
    private void build_bst(){
        System.out.println("building bst...");
        removeAll();

        Iterator i = HashToServer.entrySet().iterator();

        while(i.hasNext()){
            Map.Entry pair = (Map.Entry)i.next();
            String server = (String)pair.getValue();
            System.out.println("build: " + server);
            addServer(server);
        }
        first_node = servers_bst.first();
        last_node = servers_bst.last();
    }

    /*

    findServer( ): this function is called on KVClient side.

     */
    public String findServer(String Key){
        BigInteger keyhash = null;
        try {
            keyhash = getMD5(Key);
        }catch (Exception e){
            e.printStackTrace();
        }
        Node temp = new Node("0.0.0.0", keyhash);
        Node server_node = servers_bst.higher(temp);

        if(server_node == null)
            return first_node.id;
        else{
            return server_node.id;
        }
    }


    /*        ECS only                    */

//    public void init_hash2server(){
//        Iterator i = servers_bst.iterator();
//
//        if(i.hasNext()){
//            first_node = servers_bst.first();
//            last_node = servers_bst.last();
//        }
//
//        while (i.hasNext()) {
//            Node current_node = (Node) i.next();
//            HashToServer.put(current_node.hash, current_node.id);
//        }
//    }

    public void removeServer(String serverID){
        try {
            Node s = new Node(serverID, getMD5(serverID));
            if(servers_bst.contains(s)) {
                servers_bst.remove(s);
                first_node = servers_bst.first();
                last_node = servers_bst.last();
                HashToServer.remove(s.hash);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addServer(String serverID){
        try {
            Node s = new Node(serverID, getMD5(serverID));
            if(!servers_bst.contains(s)) {
                servers_bst.add(s);
                first_node = servers_bst.first();
                last_node = servers_bst.last();
                HashToServer.put(s.hash, s.id);
            }
            else
                System.err.println("Server already exist or Hash collosion!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void removeAll(){
        if(!servers_bst.isEmpty())
            servers_bst.clear();
    }

    public boolean isEmpty(){
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


    public void printData(String s){
        if(s.equals("bst")) {
            Iterator i = servers_bst.iterator();
            System.out.println("Printing BST...");
            while (i.hasNext()) {
                Node temp = (Node) i.next();
                System.out.println(temp.hash + ": " + temp.id);
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

    /*

    findNode( ): use id(i.e. ip:port) to find the corresponding node stored in BST.

     */

    private Node findNode(String id) {
        Iterator<Node> iterator = servers_bst.iterator();
        while(iterator.hasNext()) {
            Node node = iterator.next();
            if(node.id == id)
                return node;
        }
        return null;
    }

    public String getSuccessor(String id){

        Node n = findNode(id);
        if(n == null){
            return null;
        }
        Node successor = servers_bst.higher(n);
        if(successor == null){
            return null;
        }
        else {
            return successor.id;
        }
    }
    /*

   updateSuccessor( ) works for both adding nodes and removing nodes.
   lower doesn't change, the higher will change for all cases.

     */
    public BigInteger[] getNewSuccessorRange(String id){

        BigInteger[] successorRange;
        Node n = findNode(id);
        if(n == null){
            return null;
        }
        Node successor = servers_bst.higher(n);
        if(successor == null){
            return null;
        }
        else {
            successorRange = findHashRange(successor.id);
        }
        return successorRange;
    }

    /*
    findHashRange(): use id(i.e. ip:port) to find the corresponding hash range.
    If only one node in BST, we return low range = high range = server's hash. And this needs to be interpreted
    in KVServer side.

     */
    public BigInteger[] findHashRange(String id){

        if(!servers_bst.contains(id))
            return null;

        BigInteger[] range = new BigInteger[2];

        if(first_node.id == last_node.id){
            range[0] = first_node.hash;
            range[1] = first_node.hash;
        }
        try {
            Node curr = findNode(id);
            Node pre = servers_bst.lower(curr);

            if(pre == null){
                range[0] = last_node.hash;
                range[1] = curr.hash;
            }
            else{
                range[0] = pre.hash;
                range[1] = curr.hash;
            }

       }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        return range;
    }



    public static void main(String[] args){

        Metadata m = new Metadata();

        //test for add/remove server and update hash2server
        m.addServer("a");
        m.addServer("b");
        m.addServer("c");
        m.addServer("d");
        m.addServer("e");
        m.addServer("f");
        m.removeServer("c");
        m.removeServer("f");
       // m.update_hash2server();
        m.printData("bst");
        m.printData("h2s");


        //test for build bst from hash2server
        m.removeAll();
        if(m.isEmpty())
            System.out.println("bst empty");
        m.build_bst();
        m.printData("bst");


        //test for find server
        BigInteger test_val = new BigInteger("16955237001963240173058271559858726496");
        String server = m.findServer(String.valueOf(test_val));
        System.out.println(server);
    }
}

