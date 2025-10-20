package dev.allanbrunner.addressFormatter.address;

import java.util.Objects;
import java.util.Optional;

public record StructuredAddress(String id, String title, String name, String lastname, String firstname, String compl1,
		String compl2, AddressLine address, PostalCode postal, String city, String country) {
	public StructuredAddress {
		Objects.requireNonNull(id, "id");
		Objects.requireNonNull(address, "address");
		Objects.requireNonNull(postal, "postal");
		Objects.requireNonNull(city, "city");
		Objects.requireNonNull(country, "coutry");
	}

	public Optional<String> titleOpt() { return Optional.ofNullable(title); }

	public Optional<String> nameOpt() { return Optional.ofNullable(name); }

	public Optional<String> lastnameOpt() { return Optional.ofNullable(lastname); }

	public Optional<String> firstnameOpt() { return Optional.ofNullable(firstname); }

	public Optional<String> compl1Opt() { return Optional.ofNullable(compl1); }

	public Optional<String> compl2Opt() { return Optional.ofNullable(compl2); }
}
