package test.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class Boot {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private ExecutorService exec = Executors.newCachedThreadPool();

	private String jdbcUrl;

	private String username;

	private final ExceptionVerifier verifier = new ExceptionVerifier();

	private String password;

	private RandomSql randomSql;

	private Config config;

	private SchemaLine schemaLine;
	
	private static CommandLine line;

	public Boot(String jdbcUrl, String username, String password, String tableName, List<String> columnList,
			Config config) {
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
		this.randomSql = new RandomSql(tableName, columnList);
		this.schemaLine = new SchemaLine(config.getBase().getDdlThreads());
		this.config = config;
	}

	public static CommandLine parseOptions(String[] args) throws ParseException {

		Options options = new Options();
		options.addOption("f", true, "config file path");

		CommandLineParser parser = new BasicParser();
		CommandLine cmdLine = parser.parse(options, args);

		return cmdLine;
	}
	
	public static Config loadConfig(String filepath) throws FileNotFoundException {
		if(filepath == null) {
			return new Config();
		}
		Yaml yaml = new Yaml();
		return yaml.loadAs(new FileInputStream(new File(filepath)), Config.class);
	}
	
	public static void main(String[] args) throws ParseException, FileNotFoundException {
		
		line = parseOptions(args);
		
		Config config = loadConfig(line.getOptionValue("f"));
		
		String jdbcUrl = config.getDatabase().getJdbcUrl();
		String username = config.getDatabase().getUsername();
		String password = config.getDatabase().getPassword();
		String tableName = config.getDatabase().getTableName();
		List<String> columnNames = config.getDatabase().getColumnNames();

		Boot boot = new Boot(jdbcUrl, username, password, tableName, columnNames, config);
		boot.start();

		try {
			Thread.sleep(config.getBase().getExecuteSeconds() * 1000l);
		} catch (InterruptedException e) {
		}

		boot.stop();
	}

	public void runDDL() {

		execute(new Runner(newSession()) {
			@Override
			void doRun(Session session) {
				long time = 0l;
				try {
					DDLSql sql = randomSql.randomDDL();
					time = schemaLine.put(sql);
					session.update(sql.getQuery());

					schemaLine.toVisible(time);
				} catch (SQLException e) {
					schemaLine.remove(time);
				}
			}
		}, config.getBase().getDdlThreads());

	}

	public void runInsert() {
		execute(new Runner(newSession()) {
			@Override
			void doRun(Session session) {
				Sql sql = randomSql.randomInsert();
				try {
					long id = session.insert(sql.getQuery());
					if (id > 0) {
						Sql findByIdSql = randomSql.findById(id);
						ResultSet res = session.select(findByIdSql.getQuery());
						if (res.next()) {
							verifier.dataExist();
						} else {
							logger.warn("id:{} dose not exist.", id);
							verifier.dataNotExist();
						}
						res.close();
					}
				} catch (Exception e) {
					verifier.verify(e, sql, schemaLine.values(), schemaLine);
				}
			}
		}, config.getBase().getInsertThreads());
	}

	public void start() {

		if (!createTable()) {
			logger.warn("table may exist, stop running");
			return;
		}

		runDDL();

		runInsert();
	}

	public void stop() {
		verifier.printResult();
		exec.shutdownNow();
	}

	public boolean createTable() {
		CreateTableSql sql = randomSql.createTable();
		Session s = newSession();
		try {
			schemaLine.put(sql);
			s.update(sql.getQuery());

		} catch (SQLException e) {
			return false;
		} finally {
			if (s != null) {
				s.close();
			}
		}

		return true;
	}

	public Session newSession() {
		Session s = new Session(jdbcUrl, username, password);
		s.init();
		return s;
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

	public void execute(Runner runner, int count) {
		for (int i = 0; i < count; i++) {
			exec.execute(runner);
		}
	}

}
