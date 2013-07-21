package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.type.WideMap;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;


/**
 * BeanWithExternalJoinWideMap
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithExternalJoinWideMap
{
	public static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String name;

	@ManyToMany
	@JoinColumn(table = "external_users")
	private WideMap<Integer, UserBean> users;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public WideMap<Integer, UserBean> getUsers()
	{
		return users;
	}

	public void setUsers(WideMap<Integer, UserBean> users)
	{
		this.users = users;
	}
}
