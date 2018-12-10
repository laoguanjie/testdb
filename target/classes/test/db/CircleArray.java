package test.db;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CircleArray<T> {
	
	private final Map<Integer, T> pool = new ConcurrentHashMap<>();
	
	private final int capacity;
	
	private final AtomicInteger index = new AtomicInteger(0);
	
	public CircleArray(int capacity) {
		this.capacity = capacity;
	}
	
	public T add(T t) {
		return pool.put(index.getAndIncrement()%capacity, t);
	}
	
	public static void main(String[] args) {
		Map<String, String> m = new ConcurrentHashMap<>();
	}
}
