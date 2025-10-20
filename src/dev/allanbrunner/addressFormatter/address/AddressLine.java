package dev.allanbrunner.addressFormatter.address;

import java.util.Objects;

public abstract class AddressLine {
	private AddressLine() {}

	public static AddressLine street(String street, String houseNumber) { return new Street(street, houseNumber); }

	public static AddressLine poBox(String boxNumber) { return new PoBox(boxNumber); }

	public static final class Street extends AddressLine {
		private final String street;
		private final String houseNumber;

		private Street(String street, String houseNumber) {
			this.street = street;
			this.houseNumber = houseNumber;
		}

		public String street() { return street; }

		public String houseNumber() { return houseNumber; }

		@Override
		public String toString() {
			return "Street{" + "street='" + street + '\'' + ", houseNumber='" + houseNumber + '\'' + '}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (!(o instanceof Street street1))
				return false;
			return Objects.equals(street, street1.street) && Objects.equals(houseNumber, street1.houseNumber);
		}

		@Override
		public int hashCode() { return Objects.hash(street, houseNumber); }
	}

	public static final class PoBox extends AddressLine {
		private final String boxNumber;

		private PoBox(String boxNumber) { this.boxNumber = boxNumber; }

		public String boxNumber() { return boxNumber; }

		@Override
		public String toString() { return "PoBox{" + "boxNumber='" + boxNumber + '\'' + '}'; }

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (!(o instanceof PoBox poBox))
				return false;
			return Objects.equals(boxNumber, poBox.boxNumber);
		}

		@Override
		public int hashCode() { return Objects.hash(boxNumber); }
	}
}
