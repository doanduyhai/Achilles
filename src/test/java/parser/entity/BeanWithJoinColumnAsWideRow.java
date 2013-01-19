package parser.entity;

import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Table;

import fr.doan.achilles.entity.type.WideRow;

/**
 * BeanWithJoinColumnAsWideRow
 * 
 * @author DuyHai DOAN
 * 
 */
@Table
public class BeanWithJoinColumnAsWideRow
{
	@Id
	private Long id;

	@JoinColumn(table = "my_wide_row_cf")
	private WideRow<Integer, String> wideRow;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideRow<Integer, String> getWideRow()
	{
		return wideRow;
	}

	public void setWideRow(WideRow<Integer, String> wideRow)
	{
		this.wideRow = wideRow;
	}
}
