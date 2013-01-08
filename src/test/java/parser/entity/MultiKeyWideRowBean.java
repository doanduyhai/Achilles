package parser.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import mapping.entity.TweetMultiKey;
import fr.doan.achilles.annotations.WideRow;
import fr.doan.achilles.entity.type.WideMap;

/**
 * MultiKeyWideRowBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Table
@WideRow
public class MultiKeyWideRowBean implements Serializable
{

	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private WideMap<TweetMultiKey, String> values;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<TweetMultiKey, String> getValues()
	{
		return values;
	}

	public void setValues(WideMap<TweetMultiKey, String> values)
	{
		this.values = values;
	}
}
