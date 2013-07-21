package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.type.WideMap;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

/**
 * ClusteredEntityWithNotSupportedPropertyType
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class ClusteredEntityWithNotSupportedPropertyType
{
    @EmbeddedId
    private CompoundKey id;

    @Column(table = "xxx")
    private WideMap<Long, String> wideMap;

    public CompoundKey getId() {
        return id;
    }

    public void setId(CompoundKey id) {
        this.id = id;
    }

    public WideMap<Long, String> getWideMap() {
        return wideMap;
    }

}
