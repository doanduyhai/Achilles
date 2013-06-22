package integration.tests.entity;

import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.type.WideMap;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * WideRowEntityWithEnumCompoundKey
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@WideRow
public class WideRowEntityWithEnumCompoundKey
{
    @Id
    private Long id;

    @Column
    private WideMap<CompoundKeyWithEnum, String> map;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public WideMap<CompoundKeyWithEnum, String> getMap()
    {
        return map;
    }

}
