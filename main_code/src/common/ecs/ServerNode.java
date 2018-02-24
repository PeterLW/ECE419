package common.ecs;

public class ServerNode implements IECSNode {
    @Override
    public String getNodeName() {
        return null;
    }

    @Override
    public String getNodeHost() {
        return null;
    }

    @Override
    public int getNodePort() {
        return 0;
    }

    @Override
    public String[] getNodeHashRange() {
        return new String[0];
    }
}
