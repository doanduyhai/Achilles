package parser.entity;

import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Table;

import fr.doan.achilles.entity.type.WideRow;

/**
 * BeanWithMultiKeyJoinColumnAsWideRow
 * 
 * @author DuyHai DOAN
 * 
 */
@Table
public class BeanWithMultiKeyJoinColumnAsWideRow
{
	@Id
	private Long id;

	@JoinColumn(table = "my_wide_row_cf")
	private WideRow<CorrectMultiKey, String> wideRow;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideRow<CorrectMultiKey, String> getWideRow()
	{
		return wideRow;
	}

	public void setWideRow(WideRow<CorrectMultiKey, String> wideRow)
	{
		this.wideRow = wideRow;
	}

}
