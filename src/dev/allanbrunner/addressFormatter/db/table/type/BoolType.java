package dev.allanbrunner.addressFormatter.db.table.type;

public final class BoolType implements DataType {
	@Override
	public String sql() { return "BOOLEAN"; }
}
