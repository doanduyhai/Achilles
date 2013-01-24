package parser.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

import fr.doan.achilles.entity.type.WideMap;

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

	public WideMap<CorrectMultiKey, String> getWideRow()
	{
		return wideMap;
	}

	public void setWideRow(WideMap<CorrectMultiKey, String> wideRow)
	{
		this.wideMap = wideRow;
	}

}
