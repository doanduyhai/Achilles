package info.archinnov.achilles.test.integration.entity;

import static info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey.*;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.PartitionKey;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = TABLE_NAME)
public class EntityWithCompositePartitionKey {

	public static final String TABLE_NAME = "entity_with_composite_pk";
	@EmbeddedId
	private EmbeddedKey id;

	@Column
	private String value;

	public EntityWithCompositePartitionKey() {
	}

	public EntityWithCompositePartitionKey(Long id, String type, String value) {
		this.id = new EmbeddedKey(id, type);
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

		public EmbeddedKey() {
		}

		public EmbeddedKey(Long id, String type) {
			this.id = id;
			this.type = type;
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
	}
}
