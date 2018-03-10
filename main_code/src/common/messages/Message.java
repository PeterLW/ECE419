package common.messages;

import com.google.gson.Gson;
import common.metadata.Metadata;
import lombok.Setter;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;

public class Message implements KVMessage {

    private StatusType status;
    private transient final Gson gson = new Gson();
    @Setter private int seq = -1;
    @Setter private int clientId = -1;
    @Setter private String key;
    @Setter private String value;
    private String metadata = null;


    public Message(StatusType status, int clientId, int seq, String key,String value) {
        // might throw exception... or do message validation in Transmission class...
        this.status = status;
        this.clientId = clientId;
        this.seq = seq;
        this.key = key;
        this.value = value;

    }

    public Message(StatusType status,int clientId, int seq, String key) {
        this.status = status;
        this.clientId = clientId;
        this.seq = seq;
        this.key = key;

    }

    public Message(){}

    public void setMetaData(String metadata){

        this.metadata = metadata;
    }
    public String getMetaData(){
        return this.metadata;
    }


    /* checks to make sure valid variables are not null, for StatusType
     */
    public boolean isValid(){
        switch(status) {
            case GET:
                if (seq != -1 && clientId != -1 & key != null & value != null)
                    return true;
                break;
            case PUT:
                if (seq != -1 && clientId != -1 & key != null & value != null)
                    return true;
                break;
            default:
                if (seq != -1 && clientId != -1)
                    return true;
                break;
        }
        return false;
    }

    public int getClientID(){return this.clientId;}

    public int getSeq(){return this.seq;}

    @Override
    public String getKey(){return this.key;}

    @Override
    public String getValue(){return this.value;}

    @Override
    public StatusType getStatus(){return this.status;}

    @Override
    public ecs.IECSNode getResponsibleServer() {
        return null;
    }


}
