package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.annotations.Order;

@Entity
public class EntityWithSecondaryIndex {

	@EmbeddedId
	private EmbeddedKey id;

	@Column
	@Index
	private String label;

	@Column
	@Index
	private Integer number;

	public EntityWithSecondaryIndex() {
	}

	public EntityWithSecondaryIndex(Long id, Integer rank, String label, Integer number) {
		this.id = new EmbeddedKey(id, rank);
		this.label = label;
		this.number = number;
	}

	public EmbeddedKey getId() {
		return id;
	}

	public void setId(EmbeddedKey id) {
		this.id = id;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public Integer getNumber() {
		return number;
	}

	public void setNumber(Integer number) {
		this.number = number;
	}

	public static class EmbeddedKey {

		@Order(1)
		private Long id;

		@Order(2)
		private Integer rank;

		public EmbeddedKey() {
		}

		public EmbeddedKey(Long id, Integer rank) {
			this.id = id;
			this.rank = rank;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public Integer getRank() {
			return rank;
		}

		public void setRank(Integer rank) {
			this.rank = rank;
		}
	}
}
