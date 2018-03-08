package common.disk;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class DBManager {
    /* writes Files in
     * UTF-8 encoding
     */
    private final static String ROOT_PATH =  "DBRoot";
    private static Logger LOGGER = Logger.getLogger(DBManager.class);

    public DBManager(){
        initializeDB();
    }

    public synchronized boolean clearStorage() {
        File[] allFiles = new File(ROOT_PATH).listFiles();

        if (allFiles.length == 0)// no files in Database
            return true;

        for (File f: allFiles){
            if (!f.delete()){
                return false;
            }
        }
        return true;
    }

    public boolean isExists(String key) {
        File keyFile = new File(String.valueOf(Paths.get(ROOT_PATH,key)));
        if (keyFile.exists()){
            return true;
        }
        return false;
    }

    private synchronized boolean initializeDB(){
        File rootDirectory = new File(ROOT_PATH);
        if (rootDirectory.exists()) {
            return true;
        } else {
            rootDirectory.mkdir();
            return true;
        }
    }


    public synchronized boolean storeKV(String key, String value) {
        File keyFile = new File(String.valueOf(Paths.get(ROOT_PATH,key)));
        LOGGER.info("Attempting to store key (" + key + ") in file: " + keyFile.getAbsolutePath());

        OpenOption[] options = new OpenOption[] {StandardOpenOption.WRITE, StandardOpenOption.CREATE};
        List<String> lines = Arrays.asList(value);
        try {
            Files.write(keyFile.toPath(),lines,Charset.defaultCharset(),options);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public synchronized boolean deleteKV(String key){
        File keyFile = new File(String.valueOf(Paths.get(ROOT_PATH,key)));
        LOGGER.info("Attempting to delete key: " + key);

        if (!keyFile.exists()) {
            LOGGER.error("Attempting to delete key, but key (" + key + ") does not exist in database.");
            return false;
        }

        keyFile.delete();
        return true;
    }

    public synchronized String getKV(String key){
        File keyFile = new File(String.valueOf(Paths.get(ROOT_PATH,key)));
            LOGGER.info("Attempting to access key (" + key + ") in file: " + keyFile.getAbsolutePath());

        if (!keyFile.exists()) {
            LOGGER.error("Attempting to access key, but key (" + key + ") does not exist in database.");
            return null;
        }

        List <String> outputLines = null;
        try {
            outputLines = Files.readAllLines(keyFile.toPath());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        if (outputLines.size() == 1) {
            return outputLines.get(0);
        }

        StringJoiner sj = new StringJoiner("\n");
        for (String lines : outputLines) {
            sj.add(lines);
        }

        return sj.toString();
    }

    public ArrayList<String> returnKeysInRange(BigInteger range[]){
        File[] files = new File(ROOT_PATH).listFiles();
        ArrayList<String> keys = new ArrayList<>();
        int flag = range[0].compareTo(range[1]);
        for(File file : files){
            String filename = file.getName();
            BigInteger hash = getMD5(filename);
            System.out.println("MD5 hash of " + filename + " is " + hash);
            //move data to first node, means the node added is the first node in hash ring
            if(flag > 0){
                if(hash.compareTo(range[0]) > 0 || hash.compareTo(range[1]) < 0)
                    keys.add(filename);
            }
            else if(flag < 0){
                if(hash.compareTo(range[0]) > 0 && hash.compareTo(range[1]) < 0)
                    keys.add(filename);
            }
            else{

            }
            //System.out.println(file.getName());
        }
        return keys;
    }

    private BigInteger getMD5(String input)  {

        MessageDigest md= null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        md.update(input.getBytes(),0,input.length());
        String hash_temp = new BigInteger(1,md.digest()).toString(16);
        BigInteger hash = new BigInteger(hash_temp, 16);
        return hash;
    }



    public static void main(String[] args){
        File currDir = new File (String.valueOf(Paths.get(".")));
        System.out.println(currDir.getAbsoluteFile());

        DBManager db = new DBManager();
        db.clearStorage();
        db.storeKV("a","badsf");
        db.storeKV("b","badssf");
        db.storeKV("c","baadsf");

        BigInteger[] range = new BigInteger[2];
        range[0] = db.getMD5("a");
        range[1] = db.getMD5("d");
        System.out.println("     range from: " + range[0]);
        System.out.println("       range to: " + range[1]);
        ArrayList<String> result = db.returnKeysInRange(range);
        for(String key :result){
            System.out.println(key);
        }

//        System.out.println(db.getKV("a"));
//        System.out.println(db.getKV("abb"));
//
//        System.out.println("~~");
//
//        System.out.println(db.isExists("aaaa"));
//        db.deleteKV("aaaa");
//        System.out.println(db.isExists("aaaa"));
//
//        db.clearStorage();

    }
}
