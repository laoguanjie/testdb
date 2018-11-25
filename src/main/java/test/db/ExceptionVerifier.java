package test.db;

import java.sql.SQLException;

public class ExceptionVerifier {
	
	public void verify(Sql sql, SQLException exception) {
		String errorMsg = exception.getMessage();
	}
	
}
