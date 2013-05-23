package parser.entity;

import javax.persistence.Id;

/**
 * GrandParentBean
 * 
 * @author DuyHai DOAN
 * 
 */
public class GrandParentBean
{
	@Id
	private Long id;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

}
