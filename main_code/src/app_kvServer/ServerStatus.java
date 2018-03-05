package app_kvServer;
import ecs.ServerNode;
import java.math.BigInteger;

public class ServerStatus {

    private ServerStatusType status;
    private BigInteger[] moveRange;
    private  String targetName;

    public ServerStatus(BigInteger[] moveRange, String targetName, ServerStatusType newStatus){
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
    public void updateKVServerStatus(BigInteger[] moveRange, String newTargetName, ServerStatusType newStatus){
        this.moveRange = moveRange;
        this.targetName = newTargetName;
        this.status = newStatus;
    }


}
