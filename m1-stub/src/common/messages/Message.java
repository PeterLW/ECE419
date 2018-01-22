package common.messages;

import lombok.Setter;

public class Message implements KVMessage {
    String dummy;
    private StatusType status;
    @Setter private int seq;
    @Setter private int clientId;
    @Setter private String key;
    @Setter private String value;

    public Message(StatusType status,int clientId, int seq, String key,String value) {
        this.status = status;
        this.clientId = clientId;
        this.seq = seq;
        this.key = key;
        this.value = value;
    }

    public Message(String dummy) {
        this.dummy = dummy;
    }

    @Override
    public String getKey(){return null;}

    @Override
    public String getValue(){return null;}

    @Override
    public StatusType getStatus(){return StatusType.GET;}
}
