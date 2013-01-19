package parser.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import fr.doan.achilles.entity.type.WideRow;

/**
 * BeanWithExternalWideMap
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithExternalWideMap implements Serializable
{
	public static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String name;

	@Column(table = "external_users")
	private WideRow<Integer, UserBean> users;

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

	public WideRow<Integer, UserBean> getUsers()
	{
		return users;
	}

	public void setUsers(WideRow<Integer, UserBean> users)
	{
		this.users = users;
	}
}
