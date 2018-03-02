package ecs;

import app_kvServer.ServerStatus;
import lombok.Getter;
import lombok.Setter;

import java.math.BigInteger;

public class ServerNode implements IECSNode {
    /*
     * This is what's to be stored as data in the zNode corresponding to the KVServer
     *      serialized to JSON format before storing
     */
    private String name;
    private String host;
    private int port;
    private BigInteger[] range = new BigInteger[2];

    private transient ServerStatus statusChange; // really only used by KVServer, for ECSClient this is unreliable

    // cache values
    private int cacheSize;
    private String cacheStrategy;

    private transient String[] hexStringRange = new String[2];

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

    public String getServerName(){ // debug
        return this.name;
    }

    @Override
    public String getNodeName() {
        StringBuilder nodeName = new StringBuilder();
        nodeName.append(name);
        nodeName.append(" ");
        nodeName.append(host);
        return nodeName.toString();

    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

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
}
