package test.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Boot {

	private ExecutorService exec = Executors.newCachedThreadPool();

	private String jdbcUrl;
	
	private String username;
	
	private final LruCache<Long, Schema> schemaRecords;
	
	private String password;
	
	private RandomSql randomSql;
	
	public Boot(String jdbcUrl, String username, String password, String tableName, List<String> columnList) {
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
		this.randomSql = new RandomSql(tableName, columnList);
		this.schemaRecords = new LruCache<>(100);
	}
	
	public static void main(String[] args) {
		String jdbcUrl = "jdbc:mysql://localhost:3306/test";
		String username = "";
		String password = "";
		String tableName = "";
		String columns = "name,time";
		Boot boot = new Boot(jdbcUrl, username, password, tableName, Arrays.asList(columns.split(",")));
		boot.start();
		
		try {
			Thread.sleep(1000l);
		} catch (InterruptedException e) {
		}
		
		boot.stop();
	}
	
	public Schema lastestSchema() {
		Long max = 0l;
		for(Long timestamp : schemaRecords.keySet()) {
			if(timestamp > max) {
				max = timestamp;
			}
		}
		return schemaRecords.get(max);
	}
	
	public void updateSchema(DDLSql sql) {
		Schema s = lastestSchema().copy();
		
		if(sql.getDdlOperation().equals(DDLOperation.ADD_INDEX) || sql.getDdlOperation().equals(DDLOperation.DROP_INDEX)) {
			for(Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				s.getIndices().put(entry.getKey() + "_id", entry.getKey());
			}
		} else {
			for(Map.Entry<String, String> entry : sql.getVariables().entrySet()) {
				s.getColumns().put(entry.getKey(), entry.getValue());
			}
		}
	}
	
	public void createSchema(Map<String, String> columns, Map<String, String> indices) {
		Schema schema = new Schema();
		if(columns != null) {
			schema.getColumns().putAll(columns);
		}
		
		if(indices != null) {
			schema.getColumns().putAll(indices);
		}
		
		schemaRecords.put(System.nanoTime(), schema);
	}
	

	public void start() {
		
		if(!createTable()) {
			stop();
			return;
		}
		
		execute(new Runner(new Session(jdbcUrl, username, password)) {
			@Override
			void doRun(Session session) {
				try {
					DDLSql sql = randomSql.randomDDL();
					session.update(sql.getQuery());
					updateSchema(sql);
				} catch (SQLException e) {
				}
			}
		});

		execute(new Runner(new Session(jdbcUrl, username, password)) {
			@Override
			void doRun(Session session) {
				Sql sql = randomSql.randomInsert();
				try {
					long id = session.insert(sql.getQuery());
					if(id > 0) {
						Sql findByIdSql = randomSql.createTable();
						ResultSet res = session.select(findByIdSql.getQuery());
						if(res.next()) {
							//ok
						} else {
							// not ok
						}
					}
				} catch (SQLException e) {
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

	}
	
	public void stop() {
		exec.shutdownNow();
	}
	
	public boolean createTable() {
		Sql sql = randomSql.createTable();
		Session s = new Session(jdbcUrl, username, password);
		try {
			s.update(sql.getQuery());
		} catch (SQLException e) {
			return false;
		} finally {
			if(s != null) {
				s.close();
			}
		}
		
		return true;
	}
	

	
	abstract class Runner implements Runnable {
		
		private final Session session;
		
		public Runner(Session session) {
			this.session = session;
		}

		abstract void doRun(Session session);

		@Override
		public void run() {
			try {
				while (true) {
					doRun(session);
					Thread.sleep(1l);
				}
			} catch (InterruptedException e) {
			}
		}

	}

	public void execute(Runner runner) {
		for (int i = 0; i < 100; i++) {
			exec.execute(runner);
		}
	}
	
}
