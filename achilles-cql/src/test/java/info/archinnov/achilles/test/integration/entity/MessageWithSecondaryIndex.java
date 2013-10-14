package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.TimeUUID;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

@Entity
public class MessageWithSecondaryIndex {
	@EmbeddedId
	private EmbeddedKey id;

	@Column
	@Index
	private String label;

	@Column
	@Index
	private Integer number;

	public MessageWithSecondaryIndex() {
	}

	public MessageWithSecondaryIndex(Long id, UUID date,String label, Integer number) {
		this.id = new EmbeddedKey(id,date);
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

        @TimeUUID
        @Order(2)
        private UUID date;

        public EmbeddedKey() {
        }

        public EmbeddedKey(Long id, UUID date) {
            this.id = id;
            this.date = date;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public UUID getDate() {
            return date;
        }

        public void setDate(UUID date) {
            this.date = date;
        }
    }
}
