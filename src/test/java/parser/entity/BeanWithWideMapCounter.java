package parser.entity;

import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.WideMap;

import java.io.Serializable;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Id;

/**
 * BeanWithWideMapCounter
 * 
 * @author DuyHai DOAN
 * 
 */
public class BeanWithWideMapCounter implements Serializable
{

	private static final long serialVersionUID = 1L;

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
