package dev.allanbrunner.addressFormatter.db.table.type;

public sealed interface DataType permits IntType, DecimalType, NVarCharType, BoolType, DateType {
	String sql();

	default boolean renderAsLiteral() { return false; }
}