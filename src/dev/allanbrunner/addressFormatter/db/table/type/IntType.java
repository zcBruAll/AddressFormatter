package dev.allanbrunner.addressFormatter.db.table.type;

public final class IntType implements DataType {
	private final boolean signed;

	public IntType(boolean signed) { this.signed = signed; }

	public boolean signed() { return signed; }

	@Override
	public String sql() { return signed ? "INT" : "INT UNSIGNED"; }
}
