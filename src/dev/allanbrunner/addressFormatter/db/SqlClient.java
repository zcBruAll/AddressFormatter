package dev.allanbrunner.addressFormatter.db;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class SqlClient implements AutoCloseable {
	private final String connectionString;
	private final String user;
	private final String password;
	private final int batchSize;

	public SqlClient(String server, String port, String database, String user, String password, int batchSize)
			throws SQLException {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			throw new SQLException("SQL Server JDBC driver not available", e);
		}

		this.connectionString = String.format(
				"jdbc:sqlserver://%s:%s;database=%s;encrypt=true;trustServerCertificate=true", server, port, database);
		this.user = user;
		this.password = password;
		this.batchSize = batchSize;
	}

	public List<List<String>> executeQuery(String sql, boolean includeHeader) throws SQLException {
		try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
			statement.setFetchSize(batchSize);
			try (ResultSet resultSet = statement.executeQuery(sql)) {
				List<List<String>> results = new ArrayList<>();
				if (includeHeader) {
					ResultSetMetaData metaData = resultSet.getMetaData();
					int columnCount = metaData.getColumnCount();
					List<String> header = new ArrayList<>(columnCount);
					for (int i = 1; i <= columnCount; i++) {
						header.add(metaData.getColumnName(i));
					}
					results.add(header);
				}

				int columnCount = resultSet.getMetaData().getColumnCount();
				while (resultSet.next()) {
					List<String> row = new ArrayList<>(columnCount);
					for (int i = 1; i <= columnCount; i++) {
						String value = resultSet.getString(i);
						row.add(value);
					}
					results.add(row);
				}
				return results;
			}
		}
	}

	public long executeNonQuery(String sql) throws SQLException {
		try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
			statement.setFetchSize(batchSize);
			return statement.executeUpdate(sql);
		}
	}

	public static String sqlQuote(String value) { return value.replace("'", "''"); }

	public static String nullableQuoted(String value) {
		if (value == null || value.trim().isEmpty()) {
			return "NULL";
		}
		return "'" + sqlQuote(value.trim()) + "'";
	}

	public static String nullableRaw(String value) {
		if (value == null || value.trim().isEmpty()) {
			return "NULL";
		}
		return sqlQuote(value.trim());
	}

	private Connection openConnection() throws SQLException {
		return DriverManager.getConnection(connectionString, user, password);
	}

	@Override
	public void close() throws Exception {
		// Nothing to close explicitly; connections are scoped per call
	}
}
