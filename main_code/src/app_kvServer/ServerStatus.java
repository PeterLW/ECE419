package app_kvServer;
import java.math.BigInteger;

public class ServerStatus {
    private ServerStatusType status;
    private BigInteger[] moveRange = null;
    private String targetName = null;

    public ServerStatus(ServerStatusType newStatus, BigInteger[] moveRange, String targetName){
        this.moveRange = moveRange;
        this.targetName = targetName;
        this.status = newStatus;
    }

    public ServerStatus(ServerStatusType newStatus){
        this.status = newStatus;
    }

    public ServerStatusType getStatus(){
        return this.status;
    }
    public String getTargetName(){
        return this.targetName;
    }
    public BigInteger[] getMoveRange(){
        return this.moveRange;
    }
//
//    public void updateKVServerStatus(BigInteger[] moveRange, String newTargetName, ServerStatusType newStatus){
//        this.moveRange = moveRange;
//        this.targetName = newTargetName;
//        this.status = newStatus;
//    }
}
