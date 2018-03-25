package app_kvClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import app_kvClient.*;
import client.KVStore;
import common.messages.Message;

public class m2_benchmark  extends  Thread{

//    private String directoryName = "C:\\Users\\PeterLin\\Downloads\\maildir\\badeer-r";
        private String directoryName = "/Users/shuran/Desktop/tb/maildir/badeer-r";
//    private String directoryName = "C:\\Users\\PeterLin\\Downloads\\maildir\\blair-l";
    private ArrayList<File> files = new ArrayList<>();
    private ArrayList<String> contents = new ArrayList<>();
    private CountDownLatch countDownLatch;
    private String address;
    private  int port;

    public m2_benchmark(CountDownLatch countDownLatch, String address, int port) {

        this.countDownLatch = countDownLatch;
        this.address = address;
        this.port = port;

        listf(directoryName, this.files);
        for (File f : files) {
            String content = null;
            try {
                content = readFile(f.getAbsolutePath());
            } catch (IOException e) {
                e.printStackTrace();
            }
            contents.add(content.substring(0, 100));
            //System.out.println(f.getName());
//            System.out.println(content.substring(0, 20));
        }
        //System.out.println("number of files:" + files.size());
        //System.out.println(contents.get(0));

    }

    public void listf(String directoryName, ArrayList<File> files_) {
        File directory = new File(directoryName);

        // get all the files from a directory
        File[] fList = directory.listFiles();
        for (File file : fList) {
            if (file.isFile()) {
                files_.add(file);
            } else if (file.isDirectory()) {
                listf(file.getAbsolutePath(), files_);
            }
        }
    }

    public String readFile(String fileName) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = br.readLine();
            }
            return sb.toString();
        } finally {
            br.close();
        }
    }


    @Override
    public void run() {
        KVStore kvStore;
        while(true) {
            try {
                kvStore = new KVStore(this.address, this.port);
                kvStore.connect();
//                System.out.println("client " + kvStore.getClientId() + " start sending KV!");
                for (int i = 0; i < files.size(); i++) {
                    Message msg = kvStore.put(files.get(i).getName(), contents.get(i));
                    kvStore.updateMetadataAndResend(msg, files.get(i).getName(), contents.get(i));
                }
//                System.out.println("client " + kvStore.getClientId() + " finish!");
                kvStore.disconnect();
                break;
            } catch (IOException ioe) {
                System.out.println("Unable to connect, try reconnect!");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
        }
        countDownLatch.countDown();
    }


    public static void main(String[] args) {
        long start, end;
        int numberOfClient = 10;
        long result = 0;
        CountDownLatch latch = new CountDownLatch(numberOfClient);

        List<m2_benchmark> clients = new ArrayList<m2_benchmark>();

//        m2_benchmark bm = new m2_benchmark(latch, "localhost", 5000);

        for(int i = 0; i<numberOfClient; i++) {
            m2_benchmark bm = new m2_benchmark(latch, "localhost", 50000);
            clients.add(bm);
        }

        start = System.currentTimeMillis();
        for(m2_benchmark bm : clients) {
            bm.start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        end = System.currentTimeMillis();

        result = end - start;
        System.out.println(numberOfClient + " clients take " + result +"ms");

//        while(true){}

    }


}
