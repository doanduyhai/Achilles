package integration.tests.entity;

import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.type.WideMap;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * ColumnFamilyBeanWithObject
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@WideRow
public class WideRowEntityWithObjectProperty
{
	

	@Id
	private Long id;

	@Column
	private WideMap<Long, Holder> map;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<Long, Holder> getMap()
	{
		return map;
	}

	public void setMap(WideMap<Long, Holder> map)
	{
		this.map = map;
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