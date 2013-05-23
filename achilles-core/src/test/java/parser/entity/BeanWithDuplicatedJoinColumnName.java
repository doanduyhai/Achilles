package parser.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

/**
 * BeanWithDuplicatedJoinColumnName
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithDuplicatedJoinColumnName implements Serializable
{
	public static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String name;

	@JoinColumn(name = "name")
	private UserBean user;

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

	public UserBean getUser()
	{
		return user;
	}

	public void setUser(UserBean user)
	{
		this.user = user;
	}
}
