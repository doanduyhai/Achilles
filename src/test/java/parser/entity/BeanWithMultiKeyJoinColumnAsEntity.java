package parser.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

import fr.doan.achilles.entity.type.WideMap;

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
	private WideMap<CorrectMultiKey, Bean> wide;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<CorrectMultiKey, Bean> getWide()
	{
		return wide;
	}

	public void setWide(WideMap<CorrectMultiKey, Bean> wide)
	{
		this.wide = wide;
	}

}
