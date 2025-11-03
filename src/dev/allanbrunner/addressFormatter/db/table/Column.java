package dev.allanbrunner.addressFormatter.db.table;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import dev.allanbrunner.addressFormatter.db.table.type.DataType;

public final class Column {
	private final String name;
	private final String label; // Display name
	private final DataType type;
	private final boolean required;
	private final boolean editable;
	private final boolean visible;
	private final Map<String, Object> meta;

	private Column(Builder b) {
		this.name = Objects.requireNonNull(b.name);
		this.label = b.label != null ? b.label : b.name;
		this.type = Objects.requireNonNull(b.type);
		this.required = b.required;
		this.editable = b.editable;
		this.visible = b.visible;
		this.meta = Collections.unmodifiableMap(new LinkedHashMap<>(b.meta));
	}

	public String name() { return name; }

	public String label() { return label; }

	public String dbName() {
		Object dbn = meta.get("dbName");
		return dbn == null ? name : String.valueOf(dbn);
	}

	public DataType type() { return type; }

	public boolean required() { return required; }

	public boolean editable() { return editable; }

	public boolean visible() { return visible; }

	public Map<String, Object> meta() { return meta; }

	public static class Builder {
		private String name;
		private String label;
		private DataType type;
		private boolean required;
		private boolean editable = true;
		private boolean visible = true;
		private final Map<String, Object> meta = new LinkedHashMap<>();

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder label(String label) {
			this.label = label;
			return this;
		}

		public Builder type(DataType type) {
			this.type = type;
			return this;
		}

		public Builder required(boolean required) {
			this.required = required;
			return this;
		}

		public Builder editable(boolean editable) {
			this.editable = editable;
			return this;
		}

		public Builder visible(boolean visible) {
			this.visible = visible;
			return this;
		}

		public Builder meta(String key, Object value) {
			this.meta.put(key, value);
			return this;
		}

		public Column build() { return new Column(this); }
	}
}
