package dev.allanbrunner.addressFormatter.db;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import dev.allanbrunner.addressFormatter.db.table.Column;
import dev.allanbrunner.addressFormatter.db.table.Table;
import dev.allanbrunner.addressFormatter.db.table.type.DataType;

public final class TableSql {
	private TableSql() {}

	public static String selectTable(Table t) {
		String cols = t.columns().stream().map(Column::dbName).collect(Collectors.joining(", "));
		return "SELECT " + cols + "\nFROM " + t.name();
	}

	public static String createTable(Table t) {
		String cols = t.columns().stream().map(TableSql::columnDef).collect(Collectors.joining(",\n "));
		return "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='" + t.name() + "' AND xtype='U')\n CREATE TABLE "
				+ t.name() + " (\n " + cols + ");";
	}

	private static String columnDef(Column c) {
		String nullability = c.required() ? " NOT NULL" : "";
		DataType dt = c.type();
		return c.dbName() + " " + dt.sql() + nullability;
	}

	public static String insertSql(Table t, Map<String, Object> row) {
		List<Column> cols = t.columns();

		String colNames = cols.stream().map(Column::dbName).collect(Collectors.joining(", "));

		String values = cols.stream().map(c -> {
			Function<Object, String> formatter = c.type().renderAsLiteral()
					? (val) -> SqlClient.nullableQuoted(val == null ? null : val.toString())
					: (val) -> SqlClient.nullableRaw(val == null ? null : val.toString());

			// If column has value metadata, overrides the structured address data
			Object forced = c.meta() != null ? c.meta().get("value") : null;
			if (forced != null) {
				if (forced instanceof Supplier<?>)
					forced = ((Supplier<?>) forced).get();
				return formatter.apply(forced);
			}

			Object fromRow = row.get(c.name());

			// If nothing is set in structredAddress take default metadata
			boolean isBlank = (fromRow == null)
					|| (fromRow instanceof CharSequence && ((CharSequence) fromRow).toString().trim().isEmpty());

			if (isBlank) {
				Object def = c.meta() != null ? c.meta().get("default") : null;
				if (def != null) {
					if (def instanceof Supplier<?>)
						def = ((Supplier<?>) def).get();
					return formatter.apply(def);
				}
			}

			// Return the structuredAddress value even if null
			return formatter.apply(fromRow);
		}).collect(Collectors.joining(", "));

		return "INSERT INTO " + t.name() + " (" + colNames + ") VALUES (" + values + ");";
	}

	public static String updateSqlById(Table t, String idColumn, Map<String, Object> row) {
		List<Column> cols = t.columns().stream().filter(c -> !c.dbName().equalsIgnoreCase(idColumn)).toList();
		String setClause = cols.stream()
				.map(c -> c.dbName() + " = " + SqlClient.nullableQuoted(Objects.toString(row.get(c.dbName()), null)))
				.collect(Collectors.joining(", "));
		String where = idColumn + " = " + SqlClient.nullableQuoted(Objects.toString(row.get(idColumn), null));
		return "UPDATE " + t.name() + " SET " + setClause + " WHERE " + where + ";";
	}
}
