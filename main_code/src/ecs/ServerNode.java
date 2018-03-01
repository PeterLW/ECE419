package ecs;

import java.math.BigInteger;

public class ServerNode implements IECSNode {
    private String name;
    private String host;
    private String id; // "ipaddress:port"
    private int port;
    private BigInteger[] range = new BigInteger[];
    private transient String[] hexStringRange = new String[]; // do not serialize
    // socket?

    public ServerNode(String name, String host, int port){
        this.name = name;
        this.host = host;
        this.port = port;
        this.id = host + ":" + Integer.toString(port);
    }

    public ServerNode(ConfigEntity e){
        this.name = e.getHostName();
        this.host = e.getIpAddr();
        this.port = e.getPortNum();
    }

    public void setRange(BigInteger start, BigInteger end){
        if (start == null || end == null){
            throw new NullPointerException("start and end cannot be null");
        }
        range[0] = start;
        range[1] = end;
    }


    @Override
    public String getNodeName() {
        return name;
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
