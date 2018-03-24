package ecs;

public class ConfigEntity {

    private String hostName;
    private String ipAddr;
    private int portNum;

   public ConfigEntity(String hostName, String ipAddr, int portNum){
        this.hostName = hostName;
        this.ipAddr = ipAddr;
        this.portNum = portNum;
    }

    public String getHostName(){
        return this.hostName;
    }

    public int getPortNum(){
        return this.portNum;
    }

    public String getIpAddr(){
        return this.ipAddr;
    }


}