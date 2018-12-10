package test.db;

import java.security.SecureRandom;
import java.util.concurrent.ThreadLocalRandom;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

import javafx.scene.control.TextInputDialog;

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
			return quoted(new LocalDateTime(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE / 2)).toString("yyyy-MM-dd HH:mm:ss"));
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
		return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
	}

	private static String randomString() {
		int len = ThreadLocalRandom.current().nextInt(20);

		byte[] b = new byte[len];
		for (int i = 0; i < b.length; i++) {
			b[i] = (byte) ((ThreadLocalRandom.current().nextInt(20) + 48) & 0x7f);
		}

		return new String(b);
	}

	public String getDefaultName() {
		return defaultName;
	}

	public void setDefaultName(String defaultName) {
		this.defaultName = defaultName;
	}
	
	public static ColumnType findByName(String name) {
		for(ColumnType c : values()) {
			if(c.getDefaultName().equals(name)) {
				return c;
			}
		}
		
		throw new TestDBException("findByName should not run here");
	}
	
}
