package mapping.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import fr.doan.achilles.entity.type.WideMap;

/**
 * BeanWithObjectAsWideMapValue
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithObjectAsWideMapValue implements Serializable
{

	public static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String name;

	@Column
	private WideMap<Integer, Holder> holders;

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

	public WideMap<Integer, Holder> getHolders()
	{
		return holders;
	}

	public static class Holder implements Serializable
	{
		private static final long serialVersionUID = 1L;

		private String name;

		public Holder(String name) {
			this.name = name;
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
}
