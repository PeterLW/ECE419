package common.zookeeper;


import lombok.Getter;
import lombok.Setter;

public class ServerConfig {
    @Setter @Getter private int cacheSize;
    @Setter @Getter private String cachePolicy;

    public ServerConfig(String cachePolicy, int cacheSize){
        this.cachePolicy = cachePolicy;
        this.cacheSize = cacheSize;
    }


}
