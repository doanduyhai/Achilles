package mapping.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import fr.doan.achilles.entity.type.WideRow;

/**
 * WideRowBeanWithObject
 * 
 * @author DuyHai DOAN
 * 
 */
@fr.doan.achilles.annotations.WideRow
@Entity
public class WideRowBeanWithObject implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private WideRow<Long, Holder> map;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideRow<Long, Holder> getMap()
	{
		return map;
	}

	public void setMap(WideRow<Long, Holder> map)
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