package parser.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Table;

/**
 * ChildBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Table(name = "ChildBean")
public class ChildBean extends ParentBean implements Serializable
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
