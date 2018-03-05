package app_kvServer;
import ecs.ServerNode;
import java.math.BigInteger;

public class ServerStatus {

    private ServerNode serverNode;
    private BigInteger[] moveRange;
    private ServerStatusType status;
    private  String targetName;

    public ServerStatus(ServerNode s, BigInteger[] moveRange, String targetName, ServerStatusType newStatus){
        this.serverNode = s;
        this.moveRange = moveRange;
        this.targetName = targetName;
        this.status = newStatus;
    }

    public synchronized ServerStatusType getStatus(){
        return this.status;
    }
    public synchronized String getTargetName(){
        return this.targetName;
    }
    public synchronized BigInteger[] getMoveRange(){
        return this.moveRange;
    }
    public synchronized  ServerNode getServerNode(){
        return this.serverNode;
    }
    public void updateKVServerStatus(ServerNode newNode, BigInteger[] moveRange, String newTargetName, ServerStatusType newStatus){

        this.serverNode = newNode;
        this.moveRange = moveRange;
        this.targetName = newTargetName;
        this.status = newStatus;
    }


}
