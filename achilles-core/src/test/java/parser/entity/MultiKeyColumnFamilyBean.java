package parser.entity;

import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.type.WideMap;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import mapping.entity.TweetMultiKey;

/**
 * MultiKeyColumnFamilyBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@WideRow
public class MultiKeyColumnFamilyBean implements Serializable
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
