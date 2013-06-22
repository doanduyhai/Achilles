package parser.entity;

import info.archinnov.achilles.annotations.WideRow;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * ColumnFamilyBeanWithWrongColumnType
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@WideRow
public class WideRowBeanWithWrongColumnType
{

	

	@Id
	private Long id;

	@Column
	private String name;

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
}
