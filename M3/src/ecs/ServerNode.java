package ecs;

import app_kvServer.ServerStatus;
import java.math.BigInteger;
import java.security.MessageDigest;

public class ServerNode implements IECSNode {
    /*
     * This is what's to be stored as data in the zNode corresponding to the KVServer
     *      serialized to JSON format before storing
     */
    private String name;
    private String host;
    private int port;
    private BigInteger[] range = new BigInteger[2]; //this range is used as the coordinate range in M3
    private BigInteger[] replicaOneRange = new BigInteger[2];
    private BigInteger[] replicaTwoRange = new BigInteger[2];
    private BigInteger[] tempCoordRange = new BigInteger[2];

    // cache values
    private int cacheSize;
    private String cacheStrategy;

    // transient means it's not serialized to JSON (therefore these fields are not stored in zNode)
    private ServerStatus serverStatus = new ServerStatus(); // really only used by KVServer, for ECSClient this is unreliable
    private transient String[] hexStringRange = new String[2]; // this is only generated when accessor function is called

    public ServerNode(ConfigEntity e, int cacheSize, String cacheStrategy){
        this.name = e.getHostName();
        this.host = e.getIpAddr();
        this.port = e.getPortNum();
        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
    }

    public ServerNode(String name, String ip, int port){
        this.name = name;
        this.host = ip;
        this.port = port;
    }

    public BigInteger[] getReplicaOneRange() {
        return replicaOneRange;
    }

    public BigInteger[] getReplicaTwoRange() {
        return replicaTwoRange;
    }

    public void setReplicaOneRange(BigInteger[] newRange){

        this.replicaOneRange = newRange;
    }

    public void setReplicaTwoRange(BigInteger[] newRange){

        this.replicaTwoRange = newRange;
    }

    public BigInteger[] getRange(){
        return range;
    }

    public void setRange(BigInteger[] range){
        this.range[0] = range[0];
        this.range[1] = range[1];
    }

    public void setRange(BigInteger start, BigInteger end){
        if (start == null || end == null){
            throw new NullPointerException("start and end cannot be null");
        }
        range[0] = start;
        range[1] = end;
    }

    /*
     * A unique way to refer to Servers
     */
    @Override
    public String getNodeName() {
        return (name + "_" + host + "_" + Integer.toString(port));
    }

    public String getNodeHostPort(){
        return(host + ":" + Integer.toString(port));
    }

    public String getName(){
        return name;
    }


    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    // TODO: does this produce the right string.
    @Override
    public String[] getNodeHashRange() {
        hexStringRange[0] = range[0].toString();
        hexStringRange[1] = range[1].toString();
        return hexStringRange;
    }

    public int getCacheSize(){
        return this.cacheSize;
    }

    public String getCacheStrategy(){
        return this.cacheStrategy;
    }

    public void updateServerNode(ServerStatus newStatus){

        if(newStatus.getStatus() != null){
            serverStatus.setServerStatus(newStatus.getStatus());
        }
        if(newStatus.getTargetName() != null){
            serverStatus.setTargetName(newStatus.getTargetName());
        }
        if(newStatus.getMoveRange() != null){
            serverStatus.setMoveRange(newStatus.getMoveRange());
        }
        serverStatus.setLocalRemove(newStatus.getLocalRemove());
        serverStatus.setRangeUpdate(newStatus.getRangeUpdate());
    }

    public ServerStatus getServerStatus(){
        return serverStatus;
    }

    private BigInteger getMD5(String input) throws Exception{
        MessageDigest md=MessageDigest.getInstance("MD5");
        md.update(input.getBytes(),0,input.length());
        String hash_temp = new BigInteger(1,md.digest()).toString(16);
        BigInteger hash = new BigInteger(hash_temp, 16);
        return hash;
    }

}
