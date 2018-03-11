package ecs;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.*;
import java.util.Iterator;
import java.awt.*;
import javax.swing.*;
import java.io.*;
import com.jcraft.jsch.*;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class SSH implements Runnable {

	private String serverIpPort; 
	private String zookeeperHost;
	private String zookeeperPort;

    private static final String USER = "fucathy1"; // so you only need to change this to run on another person's computer
    private static final String PRIVATE_KEY_PATH = "/nfs/ug/homes-5/x/"+USER+"/m2files/code/main_code/";
    private static final String KNOWN_HOST_PATH = "~/.ssh/known_hosts";
    private static final int TIMEOUT = 5000;

	public SSH(String serverIpPort, String zookeeperHost, String zookeeperPort){

		this.serverIpPort = serverIpPort;
		this.zookeeperHost = zookeeperHost;
		this.zookeeperPort = zookeeperPort;
	}

	@Override
	public void run() {

		remoteLaunchServer(serverIpPort, zookeeperHost, zookeeperPort);
	}

	 private static void readChannelOutput(Channel channel){

        byte[] buffer = new byte[1024];

        try{
            InputStream in = channel.getInputStream();
            String line = "";
            while (true){
                while (in.available() > 0) {
                    int i = in.read(buffer, 0, 1024);
                    if (i < 0) {
                        break;
                    }
                    line = new String(buffer, 0, i);
                    System.out.println(line);
                }

                if(line.contains("logout")){
                    break;
                }

                if (channel.isClosed()){
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee){}
            }
        }catch(Exception e){
            System.out.println("Error while reading channel output: "+ e);
        }
    }

    public void remoteLaunchServer(String serverIpPort, String zookeeperHost, String zookeeperPort){

        JSch ssh = new JSch();
        String username;
        username = USER;
        String host = "localhost";
        String jarFilePath = "/nfs/ug/homes-5/x/"+USER+"/ECE419/m2v2/ECE419/main_code/m2-server.jar";
        StringBuilder sb=new StringBuilder("java -jar ");
        sb.append(jarFilePath);
        sb.append(" -name ");
        sb.append(serverIpPort);
        sb.append(" -zkhost ");
        sb.append(zookeeperHost);
        sb.append(" -zkport ");
        sb.append(zookeeperPort);
        String command = sb.toString();
       // String command = "java -jar /nfs/ug/homes-5/x/xushuran/ECE419/m2.jar ";
        Session session;

        try{
            ssh.setKnownHosts(KNOWN_HOST_PATH);
            session = ssh.getSession(username,host,22);
            ssh.addIdentity(PRIVATE_KEY_PATH);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect(TIMEOUT);
            System.out.println("Connected to " + username + "@" + host + ": 22");


            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);
            InputStream in = channel.getInputStream();
            channel.connect();

            readChannelOutput(channel);
            //System.out.println("Connection is closed");
            channel.disconnect();
            session.disconnect();

        }catch(JSchException | IOException e) {
            e.printStackTrace();
        }
    }

}