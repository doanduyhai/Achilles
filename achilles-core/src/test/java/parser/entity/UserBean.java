package parser.entity;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * UserBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class UserBean
{

	

	@Id
	private Long userId;

	@Column
	private String name;

	public Long getUserId()
	{
		return userId;
	}

	public void setUserId(Long userId)
	{
		this.userId = userId;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}
}
