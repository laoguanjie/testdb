package test.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class SchemaLine_backup {
	
	private ConcurrentLinkedHashMap<Long, Schema> map;
	
	private final Map<String, Object> columnsLocks = new HashMap<>();
	
	private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
	
	public void put(Long time, DDLSql sql) {
		if(sql.getDdlOperation() == null) {
			throw new TestDBException("ddl operation can not be null");
		}
		
		Schema schema = latestSchema(sql);
		modifySchema(sql, schema);
		map.put(time, schema);
	}
	
	public void remove(Long time) {
		map.remove(time);
	}
	
	public void toVisible(Long time) {
		rwlock.writeLock().lock();
		try {
			map.get(time).setReady(true);
			String columnName = getColumnName(map.get(time).getSql().getVariables());
			for(Map.Entry<Long, Schema> entry : map.entrySet()) {
				if(entry.getKey() > time && !entry.getValue().getColumns().containsKey(columnName)) {
					modifySchema(map.get(time).getSql(), entry.getValue());
				}
			}
		} finally {
			rwlock.writeLock().unlock();
		}
		
	}
	
	private String getColumnName(Map<String, String> variables) {
		if(variables.size() != 1) {
			throw new TestDBException("this size of sql variables must be 1");
		}
		
		String columnName = "";
		for(String c : variables.keySet()) {
			columnName = c;
		}
		return columnName;
	}
	
	public long modifySchema(DDLSql sql, Schema schema) {
		
		switch (sql.getDdlOperation()) {
		case ADD_INDEX:
			for(Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				if(!schema.getIndices().containsKey(entry.getKey() + "_id")) {
					schema.getIndices().put(entry.getKey() + "_id", entry.getKey());
				}
			}
			break;
		case DROP_INDEX:
			for(Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				schema.getIndices().remove(entry.getKey() + "_id");
			}
			break;
		case ADD_COLUMN:
			for(Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				if(!schema.getColumns().containsKey(entry.getKey())) {
					schema.getColumns().put(entry.getKey(), entry.getValue());
				}
			}
			break;
		case MODIFY_COLUMN:
			for(Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				if(schema.getColumns().containsKey(entry.getKey())) {
					schema.getColumns().put(entry.getKey(), entry.getValue());
				}
			}
			break;
		case DROP_COLUMN:
			for(Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				schema.getColumns().remove(entry.getKey());
			}
			break;
		default:
			break;
		}
		
		long time = System.nanoTime();
		
		return time;
	}

	
	public Schema latestSchema(DDLSql sql) {

		if(sql.getVariables().size() != 1) {
			throw new TestDBException("this size of sql variables must be 1");
		}
		
		String columnName = "";
		for(String c : sql.getVariables().keySet()) {
			columnName = c;
		}
		
		Object lock = columnsLocks.get(columnName);
		if(lock != null) {
			synchronized (lock) {
				DDLOperation operation = sql.getDdlOperation();
				switch (operation) {
				case ADD_COLUMN:
					return latestVisibleOrMatchOperationSchema(DDLOperation.DROP_COLUMN);
				case DROP_COLUMN:
				case MODIFY_COLUMN:
					return latestVisibleOrMatchOperationSchema(DDLOperation.ADD_COLUMN);
				default:
					return latestVisibleOrMatchOperationSchema(null);
				}
			}
		}
		
		throw new TestDBException("can not find column lock");
	}
	
	public Schema latestVisibleOrMatchOperationSchema(DDLOperation operation) {
		Set<Long> times = map.ascendingKeySet();
		
		for(Long time : times) {
			if(map.get(time).isReady() || map.get(time).getSql().getDdlOperation().equals(operation)) {
				return map.get(time); 
			}
		}
		
		throw new TestDBException("no visible schema");
	}
}
