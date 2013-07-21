package info.archinnov.achilles.test.parser.entity;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

/**
 * ClusteredEntity
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class ClusteredEntity
{

    @EmbeddedId
    private CompoundKey id;

    @Column
    private String value;

    public CompoundKey getId() {
        return id;
    }

    public void setId(CompoundKey id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
