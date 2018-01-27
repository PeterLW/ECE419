package common.messages;

import lombok.Setter;


public class Message implements KVMessage {

    private StatusType status;
    @Setter private int seq;
    @Setter private int clientId;
    @Setter private String key;
    @Setter private String value;

    public Message(){

    }

    public Message(final StatusType status, final int clientId, final int seq, final String key, final String value) {
        // might throw exception... or do message validation in Transmission class...
        this.status = status;
        this.clientId = clientId;
        this.seq = seq;
        this.key = key;
        this.value = value;
    }

    public Message(final StatusType status, final int clientId, final int seq, final String key) {
        this.status = status;
        this.clientId = clientId;
        this.seq = seq;
        this.key = key;
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
}
