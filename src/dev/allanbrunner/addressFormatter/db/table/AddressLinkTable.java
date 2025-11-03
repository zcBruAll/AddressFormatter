package dev.allanbrunner.addressFormatter.db.table;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import dev.allanbrunner.addressFormatter.db.table.type.Types;

public final class AddressLinkTable extends Table {
	private AddressLinkTable(String name, List<Column> columns) { super(name, columns); }

	@Override
	protected Set<String> requiredFieldNames() { return Set.of("idOriginal", "idReferenced"); }

	@Override
	protected void validateAdditionalConstraints() {}

	public static class Builder extends Table.BaseBuilder<Builder, AddressLinkTable> {
		public Builder idOriginal() { return idOriginal(null); }

		public Builder idOriginal(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("idOriginal").label("Original address Id field")
					.type(Types.NVARCHAR(32));
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		public Builder idReferenced() { return idReferenced(null); }

		public Builder idReferenced(Consumer<Column.Builder> cfg) {
			Column.Builder b = new Column.Builder().name("idReferenced").label("Referenced address Id field")
					.type(Types.INT());
			if (cfg != null)
				cfg.accept(b);
			cols.add(b.build());
			return this;
		}

		@Override
		protected Builder self() { return this; }

		@Override
		public AddressLinkTable build() { return new AddressLinkTable(name, cols); }
	}
}
