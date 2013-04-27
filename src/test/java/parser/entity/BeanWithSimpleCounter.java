package parser.entity;

import info.archinnov.achilles.entity.type.Counter;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Id;

/**
 * BeanWithSimpleCounter
 * 
 * @author DuyHai DOAN
 * 
 */
public class BeanWithSimpleCounter implements Serializable
{

	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private Counter counter;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public Counter getCounter()
	{
		return counter;
	}

}
