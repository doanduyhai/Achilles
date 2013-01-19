package parser.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import mapping.entity.TweetMultiKey;
import fr.doan.achilles.entity.type.WideRow;

/**
 * MultiKeyWideRowBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@fr.doan.achilles.annotations.WideRow
public class MultiKeyWideRowBean implements Serializable
{

	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private WideRow<TweetMultiKey, String> values;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideRow<TweetMultiKey, String> getValues()
	{
		return values;
	}

	public void setValues(WideRow<TweetMultiKey, String> values)
	{
		this.values = values;
	}
}
