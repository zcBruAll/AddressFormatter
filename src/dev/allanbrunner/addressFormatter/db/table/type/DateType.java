package dev.allanbrunner.addressFormatter.db.table.type;

public final class DateType implements DataType {
	@Override
	public String sql() { return "DATE"; }

	@Override
	public boolean renderAsLiteral() { return true; }
}
