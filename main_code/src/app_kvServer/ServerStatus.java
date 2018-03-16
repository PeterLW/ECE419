package app_kvServer;
import common.zookeeper.ZNodeMessageStatus;

import java.math.BigInteger;

public class ServerStatus {
    private ZNodeMessageStatus transition = null;
    private ServerStatusType status;
    private BigInteger[] moveRange = null;
    private BigInteger[] finalRange = null; // the range the ServerNode should have at teh end of the MoveData Operation
    private String targetName = null;
    private boolean isReady = false;

    public ServerStatus(ZNodeMessageStatus transition, BigInteger[] moveRange, String targetName, BigInteger[] finalRange){
        this.moveRange = moveRange;
        this.targetName = targetName;
        this.transition = transition;
        this.finalRange = finalRange;
    }

    public ServerStatus(){
        
    }

    public ServerStatus(ZNodeMessageStatus transition){this.transition = transition;}

    public ServerStatus(ServerStatusType newStatus){
        this.status = newStatus;
    }

    public boolean isReady(){ return isReady;}

    public void updateStatus(ServerStatus newStatus){

        this.moveRange = newStatus.getMoveRange();
        this.targetName = newStatus.getTargetName();
        this.transition = newStatus.getTransition();
    }

    public ServerStatusType getStatus(){
        return this.status;
    }
    public void setTargetName(String targetName){
         this.targetName = targetName;
    }

    public void setMoveRange(BigInteger[] newRange){
         this.moveRange = newRange;
    }

    public String getTargetName(){
        return this.targetName;
    }
    public void resetReady(){
        
        this.isReady = false;
    }
    public BigInteger[] getMoveRange(){
        return this.moveRange;
    }
    public BigInteger[] getFinalRange() { return finalRange; }

    public ZNodeMessageStatus getTransition() { return transition; }

    public void setServerStatus(ServerStatusType type){ this.status = type;}

    public void setReady(){isReady = true;}

    public void setMoveRangeStatus(ServerStatusType newStatus, BigInteger[] moveRange, String newTargetName){
        this.moveRange = moveRange;
        this.targetName = newTargetName;
        this.status = newStatus;
    }
}
