package ecs;

public class ServerNode implements IECSNode {
    private String name;
    private String host;
    private String id; // "ipaddress:port"
    private int port;
    private String[] range;
    // socket?

    public ServerNode(String name, String host, int port){
        this.name = name;
        this.host = host;
        this.port = port;
        this.id = host + ":" + Integer.toString(port);
        range = new String[2];
    }

    public void setRange(String start, String end){
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
        return range;
    }
}
