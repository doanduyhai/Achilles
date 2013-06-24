package info.archinnov.achilles.test.parser.entity;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

/**
 * ClusteredEntityWithTwoProperties
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class ClusteredEntityWithTwoProperties
{
    @EmbeddedId
    private CompoundKey id;

    @Column
    private String name;

    @Column
    private String value;

    public CompoundKey getId() {
        return id;
    }

    public void setId(CompoundKey id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
