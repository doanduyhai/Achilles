package info.archinnov.achilles.test.integration.entity;

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCompositePartitionKey.TABLE_NAME;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.PartitionKey;

@Entity(table = TABLE_NAME)
public class ClusteredEntityWithCompositePartitionKey {

	public static final String TABLE_NAME = "clustered_with_composite_pk";

	@EmbeddedId
	private EmbeddedKey id;

	@Column
	private String value;

	public ClusteredEntityWithCompositePartitionKey() {
	}

	public ClusteredEntityWithCompositePartitionKey(Long id, String type, Integer index, String value) {
		this.id = new EmbeddedKey(id, type, index);
		this.value = value;
	}

	public EmbeddedKey getId() {
		return id;
	}

	public void setId(EmbeddedKey id) {
		this.id = id;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public static class EmbeddedKey {

		@PartitionKey
		@Order(1)
		private Long id;

		@PartitionKey
		@Order(2)
		private String type;

		@Order(3)
		private Integer indexes;

		public EmbeddedKey() {
		}

		public EmbeddedKey(Long id, String type, Integer index) {
			this.id = id;
			this.type = type;
			this.indexes = index;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public Integer getIndexes() {
			return indexes;
		}

		public void setIndexes(Integer index) {
			this.indexes = index;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((indexes == null) ? 0 : indexes.hashCode());
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((type == null) ? 0 : type.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			EmbeddedKey other = (EmbeddedKey) obj;
			if (indexes == null) {
				if (other.indexes != null)
					return false;
			} else if (!indexes.equals(other.indexes))
				return false;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			if (type == null) {
				if (other.type != null)
					return false;
			} else if (!type.equals(other.type))
				return false;
			return true;
		}
	}
}
