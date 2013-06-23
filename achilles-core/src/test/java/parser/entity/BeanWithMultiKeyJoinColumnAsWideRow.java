package parser.entity;

import info.archinnov.achilles.type.WideMap;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

/**
 * BeanWithMultiKeyJoinColumnAsWideRow
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithMultiKeyJoinColumnAsWideRow
{
	@Id
	private Long id;

	@JoinColumn(table = "my_wide_row_cf")
	private WideMap<CorrectCompoundKey, String> wideMap;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<CorrectCompoundKey, String> getWideMap()
	{
		return wideMap;
	}

	public void setWideMap(WideMap<CorrectCompoundKey, String> wideMap)
	{
		this.wideMap = wideMap;
	}

}
