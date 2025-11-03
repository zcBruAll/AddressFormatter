package dev.allanbrunner.addressFormatter.db.table;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import dev.allanbrunner.addressFormatter.db.table.type.DataType;
import dev.allanbrunner.addressFormatter.db.table.type.NVarCharType;
import dev.allanbrunner.addressFormatter.db.table.type.Types;

public final class AddressFormattedTable extends Table {
	private AddressFormattedTable(String name, List<Column> columns) { super(name, columns); }

	@Override
	protected Set<String> requiredFieldNames() { return Set.of("id", "city", "country"); }

	@Override
	protected void validateAdditionalConstraints() {
		Map<String, DataType> types = columns.stream()
				.collect(java.util.stream.Collectors.toMap(Column::name, Column::type, (a, b) -> a));

		boolean postalCode = false;
		for (Map.Entry<String, DataType> entry : types.entrySet()) {
			if (entry.getKey().contains("postalCode") && !entry.getKey().equals("postalCodeSuffix")) {
				postalCode = true;
				break;
			}
		}
		if (!postalCode)
			throw new IllegalArgumentException("Missing required column: postalCode(Long)? for table " + name());

		checkIsNVarchar(types, "city");
		checkIsNVarchar(types, "country");
	}

	private void checkIsNVarchar(Map<String, DataType> map, String field) {
		DataType t = map.get(field);
		if (!(t instanceof NVarCharType)) {
			throw new IllegalArgumentException("Field '" + field + "'must be NVARCHAR");
		}
	}

	public static class Builder extends Table.BaseBuilder<Builder, AddressFormattedTable> {
		public Builder id() { return id(null); }

		public Builder id(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("id").label("Address id").type(Types.DEC(20, 0))
					.required(true);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder oldId() { return oldId(null); }

		public Builder oldId(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("oldId").label("Address old id").type(Types.NVARCHAR(64))
					.required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder title() { return title(null); }

		public Builder title(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("title").label("Address owner title").type(Types.NVARCHAR(32))
					.required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder ownerName() { return ownerName(null); }

		public Builder ownerName(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("name").label("Address owner name").type(Types.NVARCHAR(128))
					.required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder firstname() { return firstname(null); }

		public Builder firstname(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("firstname").label("Address owner firstname")
					.type(Types.NVARCHAR(64)).required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder lastname() { return lastname(null); }

		public Builder lastname(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("lastname").label("Address owner lastname")
					.type(Types.NVARCHAR(64)).required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder compl1() { return compl1(null); }

		public Builder compl1(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("compl1").label("Address first complementary line")
					.type(Types.NVARCHAR_MAX()).required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder compl2() { return compl2(null); }

		public Builder compl2(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("compl2").label("Address secondary complementary line")
					.type(Types.NVARCHAR_MAX()).required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder street() { return street(null); }

		public Builder street(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("street").label("Address street").type(Types.NVARCHAR_MAX())
					.required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder houseNumber() { return houseNumber(null); }

		public Builder houseNumber(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("houseNumber").label("Address house number")
					.type(Types.NVARCHAR(11)).required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder poBoxNumber() { return poBoxNumber(null); }

		public Builder poBoxNumber(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("poBoxNumber").label("Address postal box number")
					.type(Types.NVARCHAR(32)).required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder postalCode() { return postalCode(null); }

		public Builder postalCode(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("postalCode").label("Address postal code")
					.type(Types.DEC(4, 0)).required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder postalCodeSuffix() { return postalCodeSuffix(null); }

		public Builder postalCodeSuffix(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("postalCodeSuffix").label("Address postal code suffix")
					.type(Types.DEC(2, 0)).required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder postalCodeLong() { return postalCodeLong(null); }

		public Builder postalCodeLong(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("postalCodeLong").label("Address postal code (Long)")
					.type(Types.DEC(6, 0)).required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder city() { return city(null); }

		public Builder city(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("city").label("Address city").type(Types.NVARCHAR(64))
					.required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder country() { return country(null); }

		public Builder country(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("country").label("Address country").type(Types.NVARCHAR(2))
					.required(false);
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		@Override
		public Builder add(Column c) { return super.add(c); }

		@Override
		public Builder add(String colName, DataType type,
				Consumer<dev.allanbrunner.addressFormatter.db.table.Column.Builder> cfg) {
			return super.add(colName, type, cfg);
		}

		@Override
		protected Builder self() { return this; }

		@Override
		public AddressFormattedTable build() { return new AddressFormattedTable(name, cols); }
	}
}
