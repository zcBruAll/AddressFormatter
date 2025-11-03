package dev.allanbrunner.addressFormatter.db.table;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import dev.allanbrunner.addressFormatter.db.table.type.Types;

public final class AddressRefTable extends Table {
	private AddressRefTable(String name, List<Column> columns) { super(name, columns); }

	@Override
	protected Set<String> requiredFieldNames() { return Set.of("id", "oldId"); }

	@Override
	protected void validateAdditionalConstraints() {}

	public static class Builder extends Table.BaseBuilder<Builder, AddressRefTable> {
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

		@Override
		protected Builder self() { return this; }

		@Override
		public AddressRefTable build() { return new AddressRefTable(name, cols); }
	}
}
