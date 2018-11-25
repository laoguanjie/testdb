package test.db;

import java.util.concurrent.ThreadLocalRandom;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

public enum ColumnType {
	INT("int(11)") {
		@Override
		public String randomValue() {
			return String.valueOf(ThreadLocalRandom.current().nextInt());
		}
	},
	LONG("bigint(22)") {
		@Override
		public String randomValue() {
			return String.valueOf(ThreadLocalRandom.current().nextLong());
		}
	},
	FLOAT("float") {
		@Override
		public String randomValue() {
			return String.valueOf(ThreadLocalRandom.current().nextFloat());
		}
	},
	DOUBLE("double") {
		@Override
		public String randomValue() {
			return String.valueOf(ThreadLocalRandom.current().nextDouble());
		}
	},
	VARCHAR("varchar(255)") {
		@Override
		public String randomValue() {

			return quoted(randomString());
		}
	},
	DATE("date") {
		@Override
		public String randomValue() {
			return quoted(new LocalDate(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE / 2)).toString());
		}
	},
	DATETIME("datetime") {
		@Override
		public String randomValue() {
			return new LocalDateTime(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE / 2)).toString();
		}
	},
	CHAR("char(255)") {
		@Override
		public String randomValue() {
			return quoted(randomString());
		}
	};
	
	private String defaultName;
	
	private ColumnType(String name) {
		this.defaultName = name;
	}

	public abstract String randomValue();

	private static String quoted(String value) {
		return "'" + value + "'";
	}

	private static String randomString() {
		int len = ThreadLocalRandom.current().nextInt(65535);

		byte[] b = new byte[len];
		for (int i = 0; i < b.length; i++) {
			b[i] = (byte) ((ThreadLocalRandom.current().nextInt(127) + 1) & 0x7f);
		}

		return new String(b);
	}

	public String getDefaultName() {
		return defaultName;
	}

	public void setDefaultName(String defaultName) {
		this.defaultName = defaultName;
	}
	
}
