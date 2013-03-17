package integration.tests.entity;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ONE;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.entity.type.WideMap;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * BeanWithWriteLocalQuorumConsistencyForExternalWidemap
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Table(name = "beanWithWriteLocalQuorumWideMap")
public class BeanWithWriteLocalQuorumConsistencyForExternalWidemap implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String name;

	@Consistency(read = ONE, write = LOCAL_QUORUM)
	@Column(table = "external_widemap_with_consistency2")
	private WideMap<Integer, String> wideMap;

	public BeanWithWriteLocalQuorumConsistencyForExternalWidemap() {}

	public BeanWithWriteLocalQuorumConsistencyForExternalWidemap(Long id, String name) {
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
