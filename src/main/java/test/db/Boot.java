package test.db;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Boot {

	private ExecutorService exec = Executors.newCachedThreadPool();

	private List<Unit> units = new CopyOnWriteArrayList<>();

	class FailedReason {
		private String type;
		private Map<String, String> variables;

		public Map<String, String> getVariables() {
			return variables;
		}

		public void setVariables(Map<String, String> variables) {
			this.variables = variables;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

	}

	class Unit {
		Map<String, String> mappings;
		private String sqlType;
		private String sql;
		private boolean result;
		private String resultMsg;
		private FailedReason failedReason;

		public FailedReason getFailedReason() {
			return failedReason;
		}

		public void setFailedReason(FailedReason failedReason) {
			this.failedReason = failedReason;
		}

		public Map<String, String> getMappings() {
			return mappings;
		}

		public void setMappings(Map<String, String> mappings) {
			this.mappings = mappings;
		}

		public String getSqlType() {
			return sqlType;
		}

		public void setSqlType(String sqlType) {
			this.sqlType = sqlType;
		}

		public String getSql() {
			return sql;
		}

		public void setSql(String sql) {
			this.sql = sql;
		}

		public boolean isResult() {
			return result;
		}

		public void setResult(boolean result) {
			this.result = result;
		}

		public String getResultMsg() {
			return resultMsg;
		}

		public void setResultMsg(String resultMsg) {
			this.resultMsg = resultMsg;
		}
	}

	public static void main(String[] args) {

		// String createLocalTableTemplate = "create table if not exists {table}
		// ({columns}) engine={engine}";

	}

	public void start() {
		execute(new Runner() {
			@Override
			void doRun() {
				randomDDL();
			}
		});

		execute(new Runner() {
			@Override
			void doRun() {
				randomInsert();
			}
		});

		try {
			Thread.sleep(1000l);
		} catch (InterruptedException e) {
		}

		verify();
	}

	public String randomDDL() {
		return "";
	}

	public String randomInsert() {
		return "";
	}

	abstract class Runner implements Runnable {

		abstract void doRun();

		@Override
		public void run() {
			try {
				while (true) {

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

	public void verify() {
		for (int i = 0; i < units.size(); i++) {
			Unit u = units.get(i);
			if ("insert".equals(u.getSqlType())) {
				if (u.isResult()) {
					if (dataExist(Long.valueOf(u.getResultMsg()))) {
						// ok
					} else {
						// not ok
					}
				} else {
					FailedReason freason = u.getFailedReason();
					if ("not_map".equals(freason.getType())) {
						Map<String, String> variables = freason.getVariables();
						String field = variables.get("field");
						String value = variables.get("value");
						String columnType = variables.get("columnType");

						if (isTypeMatch(value, columnType)) {
							// not ok
						} else {
							for (int j = i - 1; j >= 0; j--) {
								Unit uu = units.get(j);
								if ("ddl".equals(uu.getSqlType()) || "create_table".equals(uu.getSqlType())) {
									Map<String, String> mappings = uu.getMappings();

								}
							}
						}
					} else if ("not_exist".equals(freason.getType())) {

					}
				}
			}
		}
	}

	public boolean isTypeMatch(String value, String columnType) {
		return false;
	}

	public boolean dataExist(long id) {
		return false;
	}

	public void sendSql(String sql) {

	}
}
