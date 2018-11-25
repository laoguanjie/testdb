package test.db;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class LruCache<K,V> {
	
	private final int MAX_CACHE_SIZE;
    LinkedHashMap<K, V> map;

	public LruCache(int cacheSize) {
    	
        MAX_CACHE_SIZE = cacheSize;
        map = new LinkedHashMap<K,V>(cacheSize + 1, 1.0f, true) {
        	
			private static final long serialVersionUID = 1L;

			@Override
            protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
                return size() > MAX_CACHE_SIZE;
            }
        };
    }

    public synchronized void put(K key, V value) {
        map.put(key, value);
    }

    public synchronized V get(K key) {
        return map.get(key);
    }

    public synchronized void remove(K key) {
        map.remove(key);
    }

    public synchronized Set<Map.Entry<K, V>> getAll() {
        return map.entrySet();
    }

    public synchronized int size() {
        return map.size();
    }

    public synchronized void clear() {
        map.clear();
    }
    
    public Set<K> keySet() {
    		return map.keySet();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry entry : map.entrySet()) {
            sb.append(String.format("%s:%s ", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }
    
}
