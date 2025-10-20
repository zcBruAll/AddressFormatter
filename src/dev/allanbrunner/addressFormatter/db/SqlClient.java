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

	public long ensureAddressTable(String table) throws SQLException {
		String sql = """
				IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='%s' AND xtype='U')
				CREATE TABLE %s (
					ID_FPR_PAYREL decimal(20,0) NOT NULL,
					FPR_PAYEMENT_DOMAIN nvarchar(20) COLLATE French_CI_AS NULL,
					FPR_ACCOUNT_OWNER_NAME nvarchar(70) COLLATE French_CI_AS NULL,
					FPR_ACCOUNT_OWNER_ADRESS_LINE1 nvarchar(70) COLLATE French_CI_AS NULL,
					FPR_ACCOUNT_OWNER_ADRESS_LINE2 nvarchar(70) COLLATE French_CI_AS NULL,
					FPR_STREET nvarchar(32) COLLATE French_CI_AS NULL,
					FPR_BUILDING_NUMBER nvarchar(11) COLLATE French_CI_AS NULL,
					FPR_POST_CODE decimal(6,0) NULL,
					FPR_TOWN_NAME nvarchar(29) COLLATE French_CI_AS NULL,
					FPR_ACCOUNT_OWNER_ADDRESS_COUNTRY nvarchar(2) COLLATE French_CI_AS NULL,
					FPR_ACCOUNT_TYPE nvarchar(10) COLLATE French_CI_AS NULL,
					FPR_ACCOUNT_NO nvarchar(40) COLLATE French_CI_AS NULL,
					FPR_CURRENCY nvarchar(3) COLLATE French_CI_AS NULL,
					FPR_PAYMENT_POOL decimal(1,0) NULL,
					FPR_ACCOUNT_NO_REF nvarchar(30) COLLATE French_CI_AS NULL,
					FPR_VALIDITY_START date NOT NULL,
					FPR_VALIDITY_END date NULL,
					FPR_STATE nvarchar(10) COLLATE French_CI_AS NOT NULL,
					FPR_SOURCE nvarchar(10) COLLATE French_CI_AS NULL,
					FPR_VALID decimal(1,0) NULL,
					FPR_USR_LOG_I nvarchar(15) COLLATE French_CI_AS NOT NULL,
					FPR_DTE_LOG_I date NOT NULL,
					FPR_USR_LOG_U nvarchar(15) COLLATE French_CI_AS NOT NULL,
					FPR_DTE_LOG_U date NOT NULL,
					OLD_TBL_ID varchar(64) NULL,
					OLD_ID_ADRESSE decimal(15,0) NULL,
					RIP_PERSON_ID decimal(10,0) NULL,
					RIP_PERSON_BPC_ID decimal(10,0) NULL,
					PAC_PAYEMENT_ADRESS_ID decimal(10,0) NULL,
					PAC_VERSION_ADR decimal(4,0) NULL
				)
				""".formatted(table, table);
		return executeNonQuery(sql);
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
