package info.archinnov.achilles.test.parser.entity;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;

/**
 * BeanWithClusteredId
 * 
 * @author DuyHai DOAN
 * 
 */
public class BeanWithClusteredId
{
    @EmbeddedId
    private CompoundKey id;

    @Column
    private String name;

    public CompoundKey getId()
    {
        return id;
    }

    public void setId(CompoundKey id)
    {
        this.id = id;
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
