package ecs;

import java.math.BigInteger;

public class ServerNode implements IECSNode {
    private String name;
    private String host;
    private String id; // "ipaddress:port"
    private int port;
    private BigInteger[] range = new BigInteger[2];
    private transient String[] hexStringRange = new String[2]; // do not serialize
    private int cacheSize;
    private String cacheStrategy;

    public ServerNode(ConfigEntity e, int cacheSize, String cacheStrategy){
        this.name = e.getHostName();
        this.host = e.getIpAddr();
        this.port = e.getPortNum();
        this.id = this.host + ":" +this.port;
        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
    }

    public ServerNode(String name, String ip, int port){
        this.name = name;
        this.host = ip;
        this.port = port;
        this.id = this.host + ":" +this.port;
    }

    public void setRange(BigInteger start, BigInteger end){
        if (start == null || end == null){
            throw new NullPointerException("start and end cannot be null");
        }
        range[0] = start;
        range[1] = end;
        hexStringRange[0] = start.toString();
        hexStringRange[1] = end.toString();
    }

    public String getServerName(){
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

    public String getNodeId(){
        return id;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    @Override
    public String[] getNodeHashRange() {
        return hexStringRange;
    }
}
