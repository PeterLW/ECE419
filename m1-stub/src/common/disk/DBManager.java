package common.disk;

import org.apache.log4j.Logger;

import javax.print.DocFlavor;
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
    private static final Logger LOGGER = Logger.getRootLogger();

    private File rootDirectory = null;

    public DBManager(){
        if (!initializeDB()){
            // throw error
        }
    }

    public boolean clearStorage() {
        return true;
    }

    public boolean isExists(String key) {
//        File keyFile =
        return true;
    }

    private boolean initializeDB(){
        if (rootDirectory != null && rootDirectory.exists()){
            return true;
        }

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
        System.out.println(keyFile.getAbsoluteFile());
        LOGGER.info("Attempting to store key" + key + " in file: " + keyFile.getAbsolutePath());

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

    public String getKV(String key){
        File keyFile = new File(String.valueOf(Paths.get(ROOT_PATH,key)));
        System.out.println(keyFile.getAbsoluteFile());
        LOGGER.info("Attempting to access key" + key + " in file: " + keyFile.getAbsolutePath());

        if (!keyFile.exists()) {
            LOGGER.error("Key " + key + " does not exist in database.");
            return "";
        }

        List <String> outputLines = null;
        try {
            outputLines = Files.readAllLines(keyFile.toPath());
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }

        if (outputLines.size() == 1) {
            return outputLines.get(0);
        }

        StringJoiner sj = new StringJoiner("\n");
//        sj.


        return "";
    }

    public static void main(String[] args){
        File currDir = new File (String.valueOf(Paths.get(".")));
        System.out.println(currDir.getAbsoluteFile());

        DBManager db = new DBManager();
        db.storeKV("a","badsf");
    }

}
