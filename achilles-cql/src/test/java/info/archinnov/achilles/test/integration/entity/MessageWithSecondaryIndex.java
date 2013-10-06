package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Index;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class MessageWithSecondaryIndex {
	@Id
	private Long id;

	@Column
	@Index
	private String label;

	@Column
	@Index
	private Integer number;

	public MessageWithSecondaryIndex() {
	}

	public MessageWithSecondaryIndex(Long id, String label, Integer number) {
		this.id = id;
		this.label = label;
		this.number = number;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
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
}
