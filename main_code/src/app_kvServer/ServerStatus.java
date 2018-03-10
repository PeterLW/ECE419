package app_kvServer;
import common.zookeeper.ZNodeMessageStatus;

import java.math.BigInteger;

public class ServerStatus {
    private ZNodeMessageStatus transition = null;
    private ServerStatusType status = null;
    private BigInteger[] moveRange = null;
    private String targetName = null;
    private boolean isReady = true;

    public ServerStatus(ZNodeMessageStatus transition, BigInteger[] moveRange, String targetName){
        this.moveRange = moveRange;
        this.targetName = targetName;
        this.transition = transition;
        this.isReady = true;
    }

    public ServerStatus(ZNodeMessageStatus transition){this.transition = transition;}

    public ServerStatus(ServerStatusType newStatus){
        this.status = newStatus;
    }

    public boolean isReady(){ return isReady;}

    public ServerStatusType getStatus(){
        return this.status;
    }
    public String getTargetName(){
        return this.targetName;
    }
    public BigInteger[] getMoveRange(){
        return this.moveRange;
    }

    public ZNodeMessageStatus getTransition() { return transition; }

    public void setServerStatus(ServerStatusType type){ this.status = status;}

    public void setReady(){isReady = true;}

    public void resetReady(){isReady = false;}

    public void setMoveRangeStatus(ServerStatusType newStatus, BigInteger[] moveRange, String newTargetName){
        this.moveRange = moveRange;
        this.targetName = newTargetName;
        this.status = newStatus;
    }
}
