package info.archinnov.achilles.test.parser.entity;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * BeanWithColumnFamilyName
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Table(name = "myOwnCF")
public class BeanWithColumnFamilyName
{
	public static final long serialVersionUID = 1234L;

	@Id
	private Long id;

	@Column
	private String name;

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
}
