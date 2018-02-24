public class ConfigEntity {

    private String hostName;
    private String portNum;
    private String ipAddr;


    ConfigEntity(String hostName, String ipAddr, String portNum){

        this.hostName = hostName;
        this.ipAddr = ipAddr;
        this.portNum = portNum;
    }

    public String getHostName(){
        return this.hostName;
    }

    public String getPortNum(){
        return this.portNum;
    }

    public String getIpAddr(){
        return this.ipAddr;
    }


}