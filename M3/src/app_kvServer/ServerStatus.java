package app_kvServer;
import common.zookeeper.ZNodeMessageStatus;

import java.math.BigInteger;

public class ServerStatus {
    private ZNodeMessageStatus transition = null;
    private ServerStatusType status;
    private BigInteger[] moveRange = null;
    private String targetName = null;
    private boolean isReady = false;
    private boolean localRemove = true;
    private boolean rangeUpdate = false;
    private BigInteger[] coordRange = new BigInteger[2];
    private BigInteger[] replicaOneRange  = new BigInteger[2];
    private BigInteger[] replicaTwoRange = new BigInteger[2];


    public ServerStatus(ZNodeMessageStatus transition, BigInteger[] moveRange, String targetName){
        this.moveRange = moveRange;
        this.targetName = targetName;
        this.transition = transition;
        this.localRemove = true; //default value
        this.rangeUpdate = false;//default value
    }

    public ServerStatus(ZNodeMessageStatus transition, BigInteger[] moveRange, String targetName, boolean localRemove, boolean rangeUpdate){
        this.moveRange = moveRange;
        this.targetName = targetName;
        this.transition = transition;
        this.localRemove = localRemove;
        this.rangeUpdate = rangeUpdate;
    }

    public ServerStatus(){
        
    }

    public void setNewRanges(BigInteger[] newCoordRange, BigInteger[] newReplicaOneRange, BigInteger[] newReplicaTwoRange){
        this.coordRange = newCoordRange;
        this.replicaOneRange = newReplicaOneRange;
        this.replicaTwoRange = newReplicaTwoRange;
    }

    public void setReplicaOneRange(BigInteger[] newRange){
        this.replicaOneRange = newRange;
    }
    public void setReplicaTwoRange(BigInteger[] newRange){
        this.replicaTwoRange = newRange;
    }

    public BigInteger[] getCoordRange() {
        return coordRange;
    }

    public BigInteger[] getReplicaOneRange() {
        return replicaOneRange;
    }

    public BigInteger[] getReplicaTwoRange() {
        return replicaTwoRange;
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
    public void setRangeUpdate(boolean rangeUpdate){
        this.rangeUpdate = rangeUpdate;
    }
    public boolean getRangeUpdate(){
        return this.rangeUpdate;
    }
    public void setLocalRemove(boolean localRemove){

        this.localRemove = localRemove;
    }
    public boolean getLocalRemove(){

        return this.localRemove;
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


    public ZNodeMessageStatus getTransition() { return transition; }

    public void setServerStatus(ServerStatusType type){ this.status = type;}

    public void setReady(){isReady = true;}

    public void setMoveRangeStatus(ServerStatusType newStatus, BigInteger[] moveRange, String newTargetName){
        this.moveRange = moveRange;
        this.targetName = newTargetName;
        this.status = newStatus;
    }
}
