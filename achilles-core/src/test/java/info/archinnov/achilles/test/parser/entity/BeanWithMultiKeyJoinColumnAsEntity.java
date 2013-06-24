package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.type.WideMap;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;


/**
 * BeanWithMultiKeyJoinColumnAsEntity
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithMultiKeyJoinColumnAsEntity
{
	@Id
	private Long id;

	@JoinColumn
	private WideMap<CorrectCompoundKey, Bean> wide;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<CorrectCompoundKey, Bean> getWide()
	{
		return wide;
	}

	public void setWide(WideMap<CorrectCompoundKey, Bean> wide)
	{
		this.wide = wide;
	}

}
