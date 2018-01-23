package common.messages;

import lombok.Setter;

public class Message implements KVMessage {
    private StatusType status;
    @Setter private int seq;
    @Setter private int clientId;
    @Setter private String key;
    @Setter private String value;

    public Message(StatusType status,int clientId, int seq, String key,String value) {
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

    /* checks to make sure valid variables are not null, for StatusType
     */
    public boolean isValid(){
        return true;
    }

    @Override
    public String getKey(){return this.key;}

    @Override
    public String getClientID(){return this.clientId;}

    @Override
    public String getSeq(){return this.seq;}

    @Override
    public String getValue(){return this.value;}

    @Override
    public StatusType getStatus(){return this.status;}
}
