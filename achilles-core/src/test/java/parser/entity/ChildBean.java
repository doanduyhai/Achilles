package parser.entity;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * ChildBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Table(name = "ChildBean")
public class ChildBean extends ParentBean
{

	public static final long serialVersionUID = 1L;

	@Column
	private String nickname;

	public String getNickname()
	{
		return nickname;
	}

	public void setNickname(String nickname)
	{
		this.nickname = nickname;
	}

}
