package common.disk;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class DBManager {
    /* writes Files in
     * UTF-8 encoding
     */
    private final static String ROOT_PATH =  "DBRoot";
    private static final Logger LOGGER = Logger.getLogger(DBManager.class);

    static {
        try {
            new logger.LogSetup("logs/storage.log", Level.DEBUG);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DBManager(){
        if (!initializeDB()){
            // throw error
        }
    }

    public boolean clearStorage() {
        File[] allFiles = new File(ROOT_PATH).listFiles();
        for (File f: allFiles){
            f.delete();
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

    private boolean initializeDB(){
        File rootDirectory = new File(ROOT_PATH);
        if (rootDirectory.exists()) {
            return true;
        } else {
            rootDirectory.mkdir();
            return true;
        }
    }

    public boolean storeKV(String key, String value) {
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

    public boolean deleteKV(String key){
        File keyFile = new File(String.valueOf(Paths.get(ROOT_PATH,key)));
        LOGGER.info("Attempting to delete key: " + key);

        if (!keyFile.exists()) {
            LOGGER.error("Attempting to delete key, but key (" + key + ") does not exist in database.");
            return false;
        }

        keyFile.delete();
        return true;
    }

    public String getKV(String key){
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


    public static void main(String[] args){
        File currDir = new File (String.valueOf(Paths.get(".")));
        System.out.println(currDir.getAbsoluteFile());

        DBManager db = new DBManager();
        db.storeKV("a","badsf");
        db.storeKV("aaaa","badssf");
        db.storeKV("asdfs","baadsf");
        System.out.println(db.getKV("a"));
        System.out.println(db.getKV("abb"));

        System.out.println("~~");

        System.out.println(db.isExists("aaaa"));
        db.deleteKV("aaaa");
        System.out.println(db.isExists("aaaa"));

        db.clearStorage();

    }
}
