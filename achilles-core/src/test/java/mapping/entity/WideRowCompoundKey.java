package mapping.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;

/**
 * WideRowMultiKey
 * 
 * @author DuyHai DOAN
 * 
 */
@CompoundKey
public class WideRowCompoundKey
{
    @Order(1)
    private Long index;

    @Order(2)
    private String name;

    public WideRowCompoundKey() {
    }

    public WideRowCompoundKey(Long index, String name) {
        this.index = index;
        this.name = name;
    }

    public Long getIndex()
    {
        return index;
    }

    public void setIndex(Long index)
    {
        this.index = index;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }
}
