package info.archinnov.achilles.test.integration.entity;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.type.WideMap;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * BeanWithReadLocalQuorumConsistencyForExternalWidemap
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Table(name = "beanWithReadLocalQuorumWideMap")
public class EntityWithReadLocalQuorumConsistencyForWidemap
{
	

	@Id
	private Long id;

	@Column
	private String name;

	@Consistency(read = LOCAL_QUORUM, write = ONE)
	@Column(table = "widemap_with_consistency3")
	private WideMap<Integer, String> wideMap;

	public EntityWithReadLocalQuorumConsistencyForWidemap() {}

	public EntityWithReadLocalQuorumConsistencyForWidemap(Long id, String name) {
		this.id = id;
		this.name = name;
	}

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

	public WideMap<Integer, String> getWideMap()
	{
		return wideMap;
	}
}
