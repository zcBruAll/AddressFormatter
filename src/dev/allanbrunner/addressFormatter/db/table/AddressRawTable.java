package dev.allanbrunner.addressFormatter.db.table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import dev.allanbrunner.addressFormatter.db.table.type.DataType;
import dev.allanbrunner.addressFormatter.db.table.type.NVarCharType;
import dev.allanbrunner.addressFormatter.db.table.type.Types;

public final class AddressRawTable extends Table {
	private AddressRawTable(String name, List<Column> columns) { super(name, columns); }

	@Override
	public List<Column> columns() {
		List<Column> linesColumns = new ArrayList<>();
		List<Column> otherColumns = new ArrayList<>();

		for (Column c : columns) {
			if (c.name().matches("line\\d+")) {
				linesColumns.add(c);
			} else {
				otherColumns.add(c);
			}
		}

		linesColumns.sort(Comparator.comparingInt(c -> {
			String name = c.name();
			return Integer.parseInt(name.replaceAll("\\D+", ""));
		}));

		otherColumns.addAll(linesColumns);

		return otherColumns;
	}

	@Override
	protected Set<String> requiredFieldNames() { return Set.of("id", "line1", "line2", "line3"); }

	@Override
	protected void validateAdditionalConstraints() {
		Map<String, DataType> types = columns.stream()
				.collect(java.util.stream.Collectors.toMap(Column::name, Column::type, (a, b) -> a));

		for (Map.Entry<String, DataType> entry : types.entrySet()) {
			boolean ok = entry.getKey().equals("id") || entry.getKey().equals("iban")
					|| entry.getKey().equals("accountOwner") || entry.getKey().matches("line\\d+");
			if (!ok)
				throw new IllegalArgumentException(
						"AddressRawTable only supports columns named 'id', 'iban', 'accountOwner' or 'line<N>'. Invalid: "
								+ entry.getKey());
		}

		checkIsNVarchar(types, "line1");
		checkIsNVarchar(types, "line2");
		checkIsNVarchar(types, "line3");
	}

	private void checkIsNVarchar(Map<String, DataType> map, String field) {
		DataType t = map.get(field);
		if (!(t instanceof NVarCharType)) {
			throw new IllegalArgumentException("Field '" + field + "'must be NVARCHAR");
		}
	}

	public static class Builder extends Table.BaseBuilder<Builder, AddressRawTable> {
		public Builder id() { return id(null); }

		public Builder id(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("id").label("Address id").type(Types.INT()).required(true);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder line(Integer idx, Integer length) { return line(idx, length, null); }

		public Builder line(Integer idx, Integer length, Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("line" + idx).label("Address Line " + idx)
					.type(Types.NVARCHAR(length)).required(true);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder iban(Integer length) { return iban(length, null); }

		public Builder iban(Integer length, Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("iban").label("Payment address IBAN")
					.type(Types.NVARCHAR(length)).required(false);
			if (cfg != null) {
				cfg.accept(b);
			}
			cols.add(b.build());
			return this;
		}

		public Builder accountOwner(Integer length) { return accountOwner(length, null); }

		public Builder accountOwner(Integer length, Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("accountOwner").label("Payment address account owner")
					.type(Types.NVARCHAR(length)).required(false);
			if (cfg != null) {
				cfg.accept(b);
			}
			cols.add(b.build());
			return this;
		}

		@Override
		protected Builder self() { return this; }

		@Override
		public AddressRawTable build() { return new AddressRawTable(name, cols); }
	}
}
