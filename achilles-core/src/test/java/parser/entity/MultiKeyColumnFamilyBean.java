package parser.entity;

import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.type.WideMap;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import mapping.entity.TweetCompoundKey;

/**
 * MultiKeyColumnFamilyBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@WideRow
public class MultiKeyColumnFamilyBean
{

	

	@Id
	private Long id;

	@Column
	private WideMap<TweetCompoundKey, String> values;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<TweetCompoundKey, String> getValues()
	{
		return values;
	}

	public void setValues(WideMap<TweetCompoundKey, String> values)
	{
		this.values = values;
	}
}
