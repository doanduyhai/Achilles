package mapping.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import fr.doan.achilles.entity.type.WideRow;

/**
 * MultiKeyWideRowBean
 * 
 * @author DuyHai DOAN
 * 
 */
@fr.doan.achilles.annotations.WideRow
@Table
public class MultiKeyWideRowBean implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private WideRow<WideRowMultiKey, String> map;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideRow<WideRowMultiKey, String> getMap()
	{
		return map;
	}

}
