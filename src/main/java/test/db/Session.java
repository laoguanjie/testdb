package test.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Session {
	
	private String jdbcUrl;
	
	private Connection conn;
	
	String username;
	
    String password; 
	
	public Session(String jdbcUrl, String username, String password) {
		this.username = username;
		this.password = password;
		this.jdbcUrl = jdbcUrl;
	}
	
	public void init() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			if(password == null || username == null) {
				conn = DriverManager.getConnection(jdbcUrl);
			} else {
				conn = DriverManager.getConnection(jdbcUrl, username, password);
			}
			
		} catch (ClassNotFoundException | SQLException e) {
			throw new TestDBException(e);
		}
	}
	
	public long insert(String sql) throws SQLException {
		Statement stmt = conn.createStatement();
		ResultSet resultSet = null;
		try {
			long i = -1;
			stmt.executeUpdate(sql);
			resultSet = stmt.getGeneratedKeys();
			while(resultSet.next()) {
				i = resultSet.getLong(1);
			}
			
			return i;
		} finally {
			if(stmt != null) {
				stmt.close();
			}
			
			if(resultSet != null) {
				resultSet.close();
			}
		}
		
	}
	
	public void update(String sql) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(sql);
	}
	
	public ResultSet select(String sql) throws SQLException {
		Statement stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}
	
	public void close() {
		if(conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
			}
		}
	}
	
}
