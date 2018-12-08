package test.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;

public class SchemaLine {
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	private ConcurrentLinkedHashMap<Long, Schema> map;

	private AtomicLong latestTime = new AtomicLong(0l);

	private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
	
	private final CircleArray<Long> circle;

	public SchemaLine(int capacity) {
		if(capacity <= 0) {
			throw new IllegalArgumentException("capacity can not less than 1");
		}
		map = new ConcurrentLinkedHashMap.Builder<Long, Schema>().maximumWeightedCapacity(capacity * 3)
				.weigher(Weighers.singleton()).build();
		
		circle = new CircleArray<>(capacity * 2);
	}

	public void put(CreateTableSql sql) {
		
		Schema schema = new Schema();
		schema.setReady(true);
		schema.getColumns().putAll(sql.getVariables());
		Long time = System.nanoTime();
		map.put(time, schema);
		circle.add(time);
		latestTime.set(time);
	}

	public Long put(DDLSql sql) {
		if (sql.getDdlOperation() == null) {
			throw new TestDBException("ddl operation can not be null.");
		}

		long time = System.nanoTime(); 
		rwlock.readLock().lock();
		try {
			
			Schema schema = latestSchema().copy();
			modifySchema(sql, schema);
			while (map.putIfAbsent(time, schema) != null) {
				time = System.currentTimeMillis();
			}
			
		} finally {
			rwlock.readLock().unlock();
		}

		return time;
	}

	public void remove(Long time) {
		map.remove(time);
	}

	public void toVisible(Long time) {
		rwlock.writeLock().lock();
		try {
			
			String columnName = getColumnName(map.get(time).getSql().getVariables());
			for (Long t : map.keySet()) {
				if (t > time) {
					Schema schema = map.get(t);
					if (schema.getColumns().containsKey(columnName)) {
						if (schema.isReady()) {
							break;
						}
					} else {
						modifySchema(map.get(time).getSql(), map.get(t));
					}
				}
			}

			Long readyTime;
			while (map.putIfAbsent((readyTime = System.nanoTime()), map.get(time)) != null) {
			}
			
			map.get(time).setReady(true);
			map.remove(time);
			
			Long t;
			if((t = circle.add(time)) != null) {
				map.remove(t);
			}
			
			long ltime;
			while ((ltime = latestTime.get()) < readyTime) {
//				logger.debug("tovisible, count={}", map.size());
				latestTime.compareAndSet(ltime, readyTime);
			}
			
		} catch (NullPointerException e) {
			logger.error("NullPointerException",e);
			logger.warn("NullPointerException, time={}, map={}",time, map);
		} finally {
			rwlock.writeLock().unlock();
		}

	}
	
	public Collection<Schema> values() {
		return map.values();
	}
	
	public Map<Long, Schema> map() {
		return map;
	}

	private String getColumnName(Map<String, String> variables) {
		if (variables.size() != 1) {
			throw new TestDBException("this size of sql variables must be 1.");
		}

		String columnName = "";
		for (String c : variables.keySet()) {
			columnName = c;
		}
		return columnName;
	}

	public void modifySchema(DDLSql sql, Schema schema) {
		schema.setSql(sql);

		switch (sql.getDdlOperation()) {
		case ADD_INDEX:
			for (Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				schema.getIndices().put(entry.getKey() + "_id", entry.getKey());
			}
			break;
		case DROP_INDEX:
			for (Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				schema.getIndices().remove(entry.getKey() + "_id");
			}
			break;
		case ADD_COLUMN:
			for (Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				schema.getColumns().put(entry.getKey(), entry.getValue());
			}
			break;
		case MODIFY_COLUMN:
			for (Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				schema.getColumns().put(entry.getKey(), entry.getValue());
			}
			break;
		case DROP_COLUMN:
			for (Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				schema.getColumns().remove(entry.getKey());
			}
			break;
		default:
			throw new TestDBException("should not be here.");
		}
		

	}

	private Schema latestSchema() {
		if (latestTime.get() == 0l) {
			throw new TestDBException("should not be zero.");
		}
		while(map.get(latestTime.get()) == null) {
			try {
				logger.debug("sleep before 1, time={},map={}", latestTime.get(), map);
				Thread.sleep(1000l);
				logger.debug("sleep after 1, time={},map={}", latestTime.get(), map);
			} catch (InterruptedException e) {
			}
		}
		return map.get(latestTime.get());
	}

	public List<Schema> rangeLatestSchema(long softStart) {
		Set<Long> times = map.keySet();
		long hardStart = 0l;
		for (Long t : times) {
			if (t < softStart && hardStart < t) {
				hardStart = t;
			}
		}
		
		for(Map.Entry<Long, Schema> entry : map.entrySet()) {
			if(entry.getValue().isReady() && entry.getKey() < softStart && hardStart < entry.getKey()) {
				hardStart = entry.getKey();
			}
		}
		
		hardStart = hardStart - 3000000000l;

		List<Schema> schemaList = new ArrayList<>();
		for(Map.Entry<Long, Schema> entry : map.entrySet()) {
			if (entry.getKey() > hardStart || !entry.getValue().isReady()) {
				schemaList.add(entry.getValue());
			}
		}
		
		return schemaList;
	}

}
