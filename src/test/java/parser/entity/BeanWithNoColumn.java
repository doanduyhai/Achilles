package parser.entity;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * BeanWithNoColumn
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
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
