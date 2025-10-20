package dev.allanbrunner.addressFormatter.address;

public final class PostalCode {
	private final int code;
	private final Integer suffix;

	public PostalCode(int code, Integer suffix) {
		this.code = code;
		this.suffix = suffix;
	}

	public int code() { return code; }

	public Integer suffix() { return suffix; }
}
