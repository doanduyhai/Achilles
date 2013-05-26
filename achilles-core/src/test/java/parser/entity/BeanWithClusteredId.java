package parser.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;

/**
 * BeanWithClusteredId
 * 
 * @author DuyHai DOAN
 * 
 */
public class BeanWithClusteredId implements Serializable
{

	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private ClusteredId id;

	@Column
	private String name;

	public ClusteredId getId()
	{
		return id;
	}

	public void setId(ClusteredId id)
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
