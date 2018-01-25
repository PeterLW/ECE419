package common;

import common.cache.Cache;
import common.disk.DBManager;

import java.awt.desktop.SystemSleepEvent;
import java.lang.*;


//NOTE： You need to make the "strategy" and "cache_size" variables in Cache Class non-static to run the benchmark
//Also, need to comment out some log message correspondingly for better visualization
//(See the output to figure out what logs need to be commented out)

public class CacheBenchmark {

    private DBManager db = new DBManager();
    private Cache fifo;
    private Cache lru;
    private Cache lfu;
    private double put_ratio = 0;
    private long times = 0;
    private int cache_size;

    public CacheBenchmark(double ratio, long times, int size) {
        if(ratio >= 0 && ratio <= 1 && times > 0) {
            this.put_ratio = ratio;
            this.times = times;
        }
        else
            System.out.println("CacheBenchmark: Invalid ratio");
        this.cache_size = size;
        fifo = new Cache(cache_size, "FIFO",db);
        lru = new Cache(cache_size, "LRU",db);
        lfu = new Cache(cache_size, "LFU",db);
    }

    public CacheBenchmark() {
        put_ratio = 0.5;
        times = 100;
        cache_size = 1000;
        fifo = new Cache(cache_size, "FIFO",db);
        lru = new Cache(cache_size, "LRU",db);
        lfu = new Cache(cache_size, "LFU",db);
    }

    public long[] run_benchmark(){
        long put_times = (long)(times * put_ratio);
        long get_times = times - put_times;
        long[] result = new long[3];

        //duplicate "for loop", avoid function call latency
        try {
            //FIFO test****************************************
            long start = System.currentTimeMillis();
            for (int i = 0; i < put_times; i++) {
                fifo.putKV(String.valueOf(i+1), String.valueOf(i+1));
            }
            for (int j = 0; j < get_times; j++) {
                fifo.getKV(String.valueOf(j+1));
            }
            long end = System.currentTimeMillis();

            result[0] = end - start;
            db.clearStorage();
            fifo.clear();

            //LRU Test*********************************************
            start = System.currentTimeMillis();
            for (int i = 0; i < put_times; i++) {
                fifo.putKV(String.valueOf(i+1), String.valueOf(i+1));
            }
            for (int j = 0; j < get_times; j++) {
                fifo.getKV(String.valueOf(j+1));
            }
            end = System.currentTimeMillis();

            result[1] = end - start;
            db.clearStorage();
            lru.clear();

            //LFU Test*************************************************
            start = System.currentTimeMillis();
            for (int i = 0; i < put_times; i++) {
                fifo.putKV(String.valueOf(i+1), String.valueOf(i+1));
            }
            for (int j = 0; j < get_times; j++) {
                fifo.getKV(String.valueOf(j+1));
            }
            end = System.currentTimeMillis();

            result[2] = end - start;
            db.clearStorage();
            lfu.clear();

        }catch(Exception e){
            System.out.println("CacheBenchmark: PUT/GET exception!");
        }

        return result;
    }

    public static void print_result(long[] result, double ratio, long times){
        System.out.println(" FIFO: Time for " + Math.ceil(ratio*times) + " putKV and " + Math.ceil((1-ratio)*times) + " getKV:" + result[0]);
        System.out.println(" LRU: Time for " + Math.ceil(ratio*times) + " putKV and " + Math.ceil((1-ratio)*times) + " getKV:" + result[1]);
        System.out.println(" LFU: Time for " + Math.ceil(ratio*times) + " putKV and " + Math.ceil((1-ratio)*times) + " getKV:" + result[2]);
    }


    public static void main(String[] args) {

        //common var
        CacheBenchmark cb;
        long[] result = new long[3];
        double ratio;
        long times;
        int cache_size = 500;

        //80% put, 20% get
        System.out.println("*************************************80% put, 20% get*******************************************");

        ratio = 0.8;

        times = 10;
        cb= new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        times = 100;
        cb = new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        times = 1000;
        cb = new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        times = 10000;
        cb = new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);


        //50% put, 50% get
        System.out.println("*************************************50% put, 50% get*******************************************");

        ratio = 0.5;

        times = 10;
        cb= new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        times = 100;
        cb = new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        times = 1000;
        cb = new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        times = 10000;
        cb = new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        //20% put, 80% get
        System.out.println("*************************************20% put, 80% get*******************************************");

        ratio = 0.2;

        times = 10;
        cb= new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        times = 100;
        cb = new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        times = 1000;
        cb = new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

        times = 10000;
        cb = new CacheBenchmark(ratio, times, cache_size);
        result = cb.run_benchmark();
        print_result(result, ratio, times);

    }

}