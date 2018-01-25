package common.cache;

public interface CacheStructure {

   boolean putKV(String key, String value);

   String getKV(String key);

   boolean deleteKV(String key);

   void clear();

   boolean inCacheStructure(String key);

   void printCacheKeys();
}
