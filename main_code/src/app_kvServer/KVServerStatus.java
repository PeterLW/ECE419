package app_kvServer;
import com.sun.security.ntlm.Server;
import ecs.ServerNode;
import java.math.BigInteger;

public class KVServerStatus {

    private ServerNode serverNode;
    private BigInteger[] moveRange;
    private ServerStatus status;
    private  String targetName;

    public KVServerStatus(ServerNode s, BigInteger[] moveRange, String targetName, ServerStatus newStatus){
        this.serverNode = s;
        this.moveRange = moveRange;
        this.targetName = targetName;
        this.status = newStatus;
    }

    public synchronized ServerStatus getStatus(){
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
    public void updateKVServerStatus(ServerNode newNode, BigInteger[] moveRange, String newTargetName, ServerStatus newStatus){

        this.serverNode = newNode;
        this.moveRange = moveRange;
        this.targetName = newTargetName;
        this.status = newStatus;
    }


}
