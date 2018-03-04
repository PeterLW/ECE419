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

    public ServerStatus getStatus(){
        return this.status;
    }
    public String getTargetName(){
        return this.targetName;
    }
    public BigInteger[] getMoveRange(){
        return this.moveRange;
    }
    public ServerNode getServerNode(){
        return this.serverNode;
    }

}
