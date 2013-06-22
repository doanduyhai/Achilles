package integration.tests.entity;

import info.archinnov.achilles.type.WideMap;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * BeanWithObjectAsWideMapValue
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class EntityWithObjectAsWideMapValue
{

	public static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private String name;

	@Column(table = "bean_with_widemap_object")
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

	public static class Holder
	{
		

		private String name;

		public Holder() {}

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
