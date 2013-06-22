package integration.tests.entity;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.WideMap;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * BeanWithConsistencyLevelOnClassAndField
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Consistency(read = ONE, write = QUORUM)
@Table(name = "entity_cl_on_class_and_widemap_counter")
public class EntityWithConsistencyLevelOnClassAndWideMapCounter
{

    @Id
    private Long id;

    @Column
    private String name;

    @Column(table = "simple_counter_wide_map")
    private WideMap<Integer, Counter> counterWideMap;

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

    public WideMap<Integer, Counter> getCounterWideMap()
    {
        return counterWideMap;
    }
}
