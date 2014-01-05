package info.archinnov.achilles.internal.persistence.metadata;

import com.google.common.base.Objects;

public class IndexProperties {

	private String name;
	private String propertyName;

	public IndexProperties(String name) {
		this.name = name;
	}

	public IndexProperties(String name, String propertyName) {
		this.name = name;
		this.propertyName = propertyName;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public void setPropertyMeta(String propertyName) {
		this.propertyName = propertyName;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("name", name).add("propertyName", propertyName).toString();
	}
}
