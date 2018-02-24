package ecs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;

public class ECSNode implements IECSNode {

    private String hostName;
    private  int portNum;
    private String lowHashRange;
    private String highHashRange;

    ECSNode(String hostName, int portNum, String lowHashRange, String highHashRange){

        this.hostName = hostName;
        this.portNum = portNum;
        this.lowHashRange = lowHashRange;
        this.highHashRange = highHashRange;

    }

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    @Override
    public String getNodeName(){

        StringBuilder nodeName = new StringBuilder();
        nodeName.append("Server ");
        nodeName.append(this.hostName);
        return nodeName.toString();
    }


    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    @Override
    public String getNodeHost(){
        return this.hostName;
    }

    /**
     * @return  the port number of the node (ie 8080)
     */
    @Override
    public int getNodePort(){
        return this.portNum;
    }

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    @Override
    public String[] getNodeHashRange(){
        String[] NodeHashRange = new String[2];
        NodeHashRange[0] = this.lowHashRange;
        NodeHashRange[1] = this.highHashRange;
        return NodeHashRange;
    }
}
