package parser.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import fr.doan.achilles.annotations.WideRow;
import fr.doan.achilles.entity.type.WideMap;

/**
 * WideRow
 * 
 * @author DuyHai DOAN
 * 
 */
@Table
@WideRow
public class WideRowBeanWithTwoColumns implements Serializable
{

	private static final long serialVersionUID = 1L;

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
