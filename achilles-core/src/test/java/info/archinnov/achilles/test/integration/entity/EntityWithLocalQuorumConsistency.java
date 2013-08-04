package info.archinnov.achilles.test.integration.entity;

import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import info.archinnov.achilles.annotations.Consistency;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * BeanWithLocalQuorumConsistency
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Consistency(read = LOCAL_QUORUM, write = LOCAL_QUORUM)
@Table(name = "consistency_test1")
public class EntityWithLocalQuorumConsistency
{

    @Id
    private Long id;

    @Column
    private String name;

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
}
