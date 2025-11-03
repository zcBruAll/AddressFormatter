package dev.allanbrunner.addressFormatter.db.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import dev.allanbrunner.addressFormatter.db.table.type.DataType;

public abstract class Table {
	protected final String name;
	protected final List<Column> columns;

	protected Table(String name, List<Column> columns) {
		this.name = Objects.requireNonNull(name);
		this.columns = List.copyOf(columns);
		validateRequiredColumns();
	}

	public String name() { return name; }

	public List<Column> columns() { return columns; }

	public Column findColumn(String name) {
		for (Column c : columns()) {
			if (c.name().equals(name)) {
				return c;
			}
		}
		return null;
	}

	protected abstract Set<String> requiredFieldNames();

	protected void validateAdditionalConstraints() {}

	private void validateRequiredColumns() {
		Set<String> present = columns.stream().map(Column::name).collect(Collectors.toSet());
		for (String req : requiredFieldNames()) {
			if (!present.contains(req)) {
				throw new IllegalArgumentException("Missing required column: " + req + " for table " + name);
			}
		}
		validateAdditionalConstraints();
	}

	public static abstract class BaseBuilder<B extends BaseBuilder<B, T>, T extends Table> {
		protected String name = "defaultName";
		protected final List<Column> cols = new ArrayList<>();

		public B name(String name) {
			this.name = name;
			return self();
		}

		protected B add(Column c) {
			cols.add(c);
			return self();
		}

		protected B add(String colName, DataType type, Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name(colName).type(type);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return self();
		}

		protected abstract B self();

		public abstract T build();
	}
}
