package dev.allanbrunner.addressFormatter.db.table.type;

public final class NVarCharType implements DataType {
	private final Integer length;

	public NVarCharType(Integer length) {
		if (length == 0 || length < -1)
			throw new IllegalArgumentException("Invalid NVARCHAR length");
		this.length = length;
	}

	public Integer length() { return length; }

	@Override
	public String sql() { return length == -1 ? "NVARCHAR(MAX)" : "NVARCHAR(" + length + ")"; }

	@Override
	public boolean renderAsLiteral() { return true; }
}
