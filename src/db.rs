use anyhow::Error;
use odbc_api::{                             // Import types from the odbc_api crate:
    buffers::TextRowSet,                    //   TextRowSet: buffer type for fetching rows in batches as text.
    ConnectionOptions,                      //   ConnectionOptions: options for opening ODBC connections.
    Cursor,                                 //   Cursor: for iterating over query results.
    Environment,                            //   Environment: the ODBC environment; must be created once per process.
    ResultSetMetadata,                      //   ResultSetMetadata: metadata about columns (used for names).
};

/// A simple SQL Server client using ODBC.
pub struct SqlClient {
    env: Environment,
    conn_str: String,
    batch_size: usize,
}

impl SqlClient {
    /// Create a new client, building an ODBC connection string.
    ///
    /// ## Arguments
    ///
    /// * `server`     – the hostname or IP of the SQL Server instance
    /// * `port`       – the TCP port on which SQL Server is listening
    /// * `name`       – the target database name
    /// * `user`       – the login user name (UID)
    /// * `pwd`        – the password (PWD)
    /// * `batch_size` – how many rows to fetch per round‐trip
    ///
    /// ## Returns
    ///
    /// A configured `SqlClient` on success.
    pub fn new(
        server: &str,
        port: &str,
        name: &str,
        user: &str,
        pwd: &str,
        batch_size: usize,
    ) -> Result<Self, Error> {
        // Build the ODBC Environment handle.
        let env = Environment::new()?;

        // Construct the connection string with the provided parameters.
        //    - Uses Microsoft ODBC Driver 18 for SQL Server
        //    - TrustServerCertificate=Yes to skip TLS cert validation
        let conn_str = format!(
            "Driver={{ODBC Driver 18 for SQL Server}};\
             Server={},{};\
             Database={};\
             UID={};\
             PWD={};\
             TrustServerCertificate=Yes;",
            server, port, name, user, pwd
        );

        // Return the new client struct.
        Ok(SqlClient {
            env,
            conn_str,
            batch_size,
        })
    }

    /// Execute a SQL SELECT query and return all rows (and optionally the header).
    ///
    /// ## Arguments
    ///
    /// * `sql` – the SQL query string to execute.
    /// * `include_header` – if `true`, the first row of the returned `Vec<Vec<String>>`
    ///   will be the column names.
    ///
    /// ## Returns
    ///
    /// On success, a `Vec` of rows, where each row is a `Vec<String>` of column values.
    /// If `include_header` is `true`, the first inner `Vec` contains the column names.
    pub fn execute_query(
        &self,
        sql: &str,
        include_header: bool,
    ) -> Result<Vec<Vec<String>>, Error> {
        // Open a new connection using the stored connection string.
        let conn = self
            .env
            .connect_with_connection_string(
                &self.conn_str,
                ConnectionOptions::default(),
            )?;

        // Prepare output container for rows (and optional header).
        let mut out = Vec::new();

        // Execute the query; if it returns a result set, we get a cursor.
        if let Some(mut cursor) = conn.execute(sql, (), None)? {
            // 3. Optionally fetch column names and push them as the first "row".
            if include_header {
                let cols = cursor
                    .column_names()?                             // Iterator over &str
                    .collect::<Result<Vec<String>, _>>()?;       // Collect into Vec<String>
                out.push(cols);
            }

            // Prepare a text‐row buffer for batch fetching.
            //    `batch_size` controls how many rows to retrieve per round‐trip.
            //    The `Some(4096)` is the max total byte size per batch.
            let mut buffers =
                TextRowSet::for_cursor(self.batch_size, &mut cursor, Some(4096))?;

            // Bind the buffer to the cursor, returning a Rows iterator.
            let mut rows = cursor.bind_buffer(&mut buffers)?;

            // Loop until no more batches.
            while let Some(batch) = rows.fetch()? {
                // For each row in this batch...
                for row_index in 0..batch.num_rows() {
                    // Build a Vec<String> of the columns in this row.
                    let row = (0..batch.num_cols())
                        .map(|col_index| {
                            // Get raw bytes for (col, row), convert to UTF‑8 string.
                            std::str::from_utf8(batch.at(col_index, row_index).unwrap_or(&[]))
                                .unwrap_or("")      // on invalid UTF‑8, use empty string
                                .to_string()
                        })
                        .collect();
                    out.push(row);
                }
            }
        }

        // Return all rows (and header if requested).
        Ok(out)
    }

    /// Execute a non-SELECT SQL statement (INSERT, UPDATE, DELETE, DDL) and return
    /// the number of rows affected.
    ///
    /// ## Arguments
    ///
    /// * `sql` – the SQL statement to execute.
    ///
    /// ## Returns
    ///
    /// On success, the count of rows affected by the statement.
    pub fn execute_non_query(&self, sql: &str) -> Result<u64, Error> {
        // Open a new connection.
        let conn = self
            .env
            .connect_with_connection_string(
                &self.conn_str,
                ConnectionOptions::default(),
            )?;

        // Prepare the statement for execution.
        let mut prepared = conn.prepare(sql)?;

        // Execute it; we ignore any returned result set.
        prepared.execute(())?;

        // Ask ODBC how many rows were affected (may be None).
        let affected = prepared
            .row_count()?               // Option<i32>
            .unwrap_or(0)               // treat None as 0 rows affected
            .try_into()                 // convert i32 → u64
            .unwrap();                  // should never fail, but panic if it does

        // Return the affected‐row count.
        Ok(affected)
    }
}
