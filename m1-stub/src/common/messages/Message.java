package common.messages;

public class Message implements KVMessage {
    String dummy;

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