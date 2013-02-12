package parser.entity;

import info.archinnov.achilles.entity.type.WideMap;

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
	private WideMap<CorrectMultiKey, String> wideMap;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<CorrectMultiKey, String> getWideMap()
	{
		return wideMap;
	}

	public void setWideMap(WideMap<CorrectMultiKey, String> wideMap)
	{
		this.wideMap = wideMap;
	}

}
