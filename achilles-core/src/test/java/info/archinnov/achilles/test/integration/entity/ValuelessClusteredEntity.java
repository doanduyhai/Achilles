package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Order;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

/**
 * ValuelessClusteredEntity
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class ValuelessClusteredEntity {

    @EmbeddedId
    private CompoundKey id;

    public ValuelessClusteredEntity() {
    }

    public ValuelessClusteredEntity(CompoundKey id) {
        this.id = id;
    }

    public CompoundKey getId() {
        return id;
    }

    public void setId(CompoundKey id) {
        this.id = id;
    }

    public static class CompoundKey
    {
        @Order(1)
        private Long id;

        @Order(2)
        private String name;

        public CompoundKey() {
        }

        public CompoundKey(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
