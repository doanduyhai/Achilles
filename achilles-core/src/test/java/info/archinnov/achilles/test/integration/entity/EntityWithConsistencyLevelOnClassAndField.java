package info.archinnov.achilles.test.integration.entity;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.type.Counter;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * EntityWithConsistencyLevelOnClassAndField
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Consistency(read = LOCAL_QUORUM, write = QUORUM)
public class EntityWithConsistencyLevelOnClassAndField
{

    @Id
    private Long id;

    @Column
    private String name;

    @Column
    @Consistency(read = ONE, write = ONE)
    private Counter count;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
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

    public Counter getCount()
    {
        return count;
    }

    public void setCount(Counter count) {
        this.count = count;
    }
}
