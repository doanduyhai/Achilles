package parser.entity;

import java.io.Serializable;

import javax.persistence.Id;
import javax.persistence.Table;

/**
 * BeanWithNoColumn
 * 
 * @author DuyHai DOAN
 * 
 */
@Table
public class BeanWithNoColumn implements Serializable
{
	public static final long serialVersionUID = 1L;

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
