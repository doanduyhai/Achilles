package integration.tests.entity;

import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.type.WideMap;



import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;

/**
 * ColumnFamilyBeanWithJoinEntity
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@WideRow
public class WideRowEntityWithJoinEntity
{
	

	@Id
	private Long id;

	@JoinColumn
	@ManyToMany(cascade = CascadeType.ALL)
	private WideMap<Integer, User> friends;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<Integer, User> getFriends()
	{
		return friends;
	}
}
