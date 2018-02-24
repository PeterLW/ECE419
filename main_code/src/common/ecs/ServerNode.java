package common.ecs;

public class ServerNode implements IECSNode {
    private String name;
    private String host;
    private String id;
    private int port;

    public ServerNode(String name, String host, int port){
        this.name = name;
        this.host = host;
        this.port = port;
        this.id = host + ":" + Integer.toString(port);
    }

    @Override
    public String getNodeName() {
        return name;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    public String getNodeHostPort(){
        return id;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    @Override
    public String[] getNodeHashRange() {
        return new String[0];
    }
}
