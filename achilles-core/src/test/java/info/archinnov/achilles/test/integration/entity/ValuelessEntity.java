package info.archinnov.achilles.test.integration.entity;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * ValuelessEntity
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class ValuelessEntity {

    @Id
    private Long id;

    public ValuelessEntity() {
    }

    public ValuelessEntity(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

}
