package parser.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import fr.doan.achilles.entity.type.WideRow;

/**
 * WideRowBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Table
@fr.doan.achilles.annotations.WideRow
public class WideRowBean implements Serializable
{

	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private WideRow<Integer, String> values;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideRow<Integer, String> getValues()
	{
		return values;
	}

	public void setValues(WideRow<Integer, String> values)
	{
		this.values = values;
	}
}
