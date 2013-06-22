package parser.entity;

import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.type.WideMap;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * ColumnFamilyBeanWithTwoColumns
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@WideRow
public class WideRowBeanWithTwoColumns
{

	

	@Id
	private Long id;

	@Column
	private String name;

	@Column
	private WideMap<Integer, String> values;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<Integer, String> getValues()
	{
		return values;
	}

	public void setValues(WideMap<Integer, String> values)
	{
		this.values = values;
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
