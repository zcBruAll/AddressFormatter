package dev.allanbrunner.addressFormatter.db.table.type;

public final class DecimalType implements DataType {
	private final Integer precision;
	private final Integer scale;

	public DecimalType(Integer precision, Integer scale) {
		if (precision <= 0 || scale < 0 || scale > precision)
			throw new IllegalArgumentException("Invalid DECIMAL(p, s)");
		this.precision = precision;
		this.scale = scale;
	}

	public Integer precision() { return precision; }

	public Integer scale() { return scale; }

	@Override
	public String sql() { return "DECIMAL(" + precision + ", " + scale + ")"; }
}
