package integration.tests.entity;

import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.type.WideMap;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * WideRowEntityWithCompoundKey
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@WideRow
public class WideRowEntityWithCompoundKey
{
    @Id
    private Long id;

    @Column
    private WideMap<CompoundKey, String> map;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public WideMap<CompoundKey, String> getMap()
    {
        return map;
    }

}
