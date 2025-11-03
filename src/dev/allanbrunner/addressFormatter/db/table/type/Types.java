package dev.allanbrunner.addressFormatter.db.table.type;

public final class Types {
	public static IntType INT() { return new IntType(true); }

	public static DecimalType DEC(Integer p, Integer s) { return new DecimalType(p, s); }

	public static NVarCharType NVARCHAR(Integer length) { return new NVarCharType(length); }

	public static NVarCharType NVARCHAR_MAX() { return new NVarCharType(-1); }

	public static BoolType BOOL() { return new BoolType(); }

	public static DateType DATE() { return new DateType(); }
}