package parser.entity;

import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.WideMap;


import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Id;

/**
 * BeanWithWideMapCounter
 * 
 * @author DuyHai DOAN
 * 
 */
public class BeanWithWideMapCounter
{

	

	@Id
	private Long id;

	@Column(table = "counters")
	private WideMap<UUID, Counter> counters;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<UUID, Counter> getCounters()
	{
		return counters;
	}

}
