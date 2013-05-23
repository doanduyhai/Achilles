package parser.entity;

import info.archinnov.achilles.type.WideMap;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

/**
 * BeanWithJoinColumnAsWideMap
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithJoinColumnAsWideMap
{
	@Id
	private Long id;

	@JoinColumn(table = "my_wide_row_cf")
	private WideMap<Integer, String> wideMap;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<Integer, String> getWideRow()
	{
		return wideMap;
	}

	public void setWideRow(WideMap<Integer, String> wideRow)
	{
		this.wideMap = wideRow;
	}
}
