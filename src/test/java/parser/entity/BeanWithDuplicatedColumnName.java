package parser.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * BeanWithDuplicatedColumnName
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithDuplicatedColumnName implements Serializable
{
	public static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String name;

	@Column(name = "name")
	private String duplicatedName;

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

	public String getDuplicatedName()
	{
		return duplicatedName;
	}

	public void setDuplicatedName(String duplicatedName)
	{
		this.duplicatedName = duplicatedName;
	}
}
