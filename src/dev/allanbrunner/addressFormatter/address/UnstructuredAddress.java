package dev.allanbrunner.addressFormatter.address;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class UnstructuredAddress {
	private final String id;
	private final List<String> lines;
	private final String iban;
	private final String accountOwner;

	public UnstructuredAddress(String id, String[] lines) { this(id, lines, "", ""); }

	public UnstructuredAddress(String id, String[] lines, String iban, String accountOwner) {
		this.id = Objects.requireNonNull(id, "id");
		if (lines.length != 6) {
			throw new IllegalArgumentException("Expected exactly six address lines");
		}
		this.lines = List.copyOf(Arrays.asList(lines));
		this.iban = iban;
		this.accountOwner = accountOwner;
	}

	public String id() { return id; }

	public List<String> lines() { return lines; }

	public String iban() { return iban; }

	public String accountOwner() { return accountOwner; }
}
