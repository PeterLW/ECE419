package app_kvServer;
import common.zookeeper.ZNodeMessageStatus;

import java.math.BigInteger;

public class ServerStatus {
    public ZNodeMessageStatus transition = null;
    private ServerStatusType status = null;
    private BigInteger[] moveRange = null;
    private BigInteger[] finalRange = null; // the range the ServerNode should have at teh end of the MoveData Operation
    private String targetName = null;
    private boolean isReady = false;

    public ServerStatus(ZNodeMessageStatus transition, BigInteger[] moveRange, String targetName, BigInteger[] finalRange){
        this.moveRange = moveRange;
        this.targetName = targetName;
        this.transition = transition;
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
    public BigInteger[] getFinalRange() { return finalRange; }

    public ZNodeMessageStatus getTransition() { return transition; }

    public void updateServerStatus(ServerStatus status){
        this.status = status.status;
        this.moveRange = status.moveRange;
        this.targetName = status.targetName;
        this.finalRange = status.finalRange;
        this.isReady = false;
    }

    public void setServerStatus(ServerStatusType type){ this.status = type;}

    public void setReady(){isReady = true;}

    public void setMoveRangeStatus(ServerStatusType newStatus, BigInteger[] moveRange, String newTargetName){
        this.moveRange = moveRange;
        this.targetName = newTargetName;
        this.status = newStatus;
    }
}
