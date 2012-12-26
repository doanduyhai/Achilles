package integration.tests.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import fr.doan.achilles.annotations.WideRow;
import fr.doan.achilles.entity.type.WideMap;

/**
 * WideRowBean
 * 
 * @author DuyHai DOAN
 * 
 */
@WideRow
@Table
public class WideRowBeanWithObject implements Serializable
{
	private static final long serialVersionUID = 1L;

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

	public static class Holder implements Serializable
	{
		private static final long serialVersionUID = 1L;
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