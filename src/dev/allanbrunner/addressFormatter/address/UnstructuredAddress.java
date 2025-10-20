package dev.allanbrunner.addressFormatter.address;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class UnstructuredAddress {
	private final String id;
	private final List<String> lines;

	public UnstructuredAddress(String id, String[] lines) {
		this.id = Objects.requireNonNull(id, "id");
		if (lines.length != 6) {
			throw new IllegalArgumentException("Expected exactly six address lines");
		}
		this.lines = List.copyOf(Arrays.asList(lines));
	}

	public String id() { return id; }

	public List<String> lines() { return lines; }
}
