package integration.tests.entity;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.WideMap;
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

    @Consistency(read = ONE, write = ONE)
    @Column(table = "widemap_with_consistency_override")
    private WideMap<Integer, String> wideMap;

    @Consistency(read = ONE, write = EACH_QUORUM)
    @Column(table = "widemap_with_each_quorum_write")
    private WideMap<Integer, String> wideMapEachQuorumWrite;

    @Column(table = "counter_widemap")
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

    public Counter getCount()
    {
        return count;
    }

    public WideMap<Integer, String> getWideMap()
    {
        return wideMap;
    }

    public WideMap<Integer, String> getWideMapEachQuorumWrite()
    {
        return wideMapEachQuorumWrite;
    }

    public WideMap<Integer, Counter> getCounterWideMap()
    {
        return counterWideMap;
    }
}
