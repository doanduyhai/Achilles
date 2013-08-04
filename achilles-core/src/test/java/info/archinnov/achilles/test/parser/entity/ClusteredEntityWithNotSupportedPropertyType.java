package info.archinnov.achilles.test.parser.entity;

import java.util.Map;
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
    private Map<Long, String> map;

    public CompoundKey getId() {
        return id;
    }

    public void setId(CompoundKey id) {
        this.id = id;
    }

    public Map<Long, String> getMap() {
        return map;
    }

    public void setMap(Map<Long, String> map) {
        this.map = map;
    }

}
