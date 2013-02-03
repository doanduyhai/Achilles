package integration.tests.entity;

import info.archinnov.achilles.annotations.ColumnFamily;
import info.archinnov.achilles.entity.type.WideMap;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;


/**
 * WideRowBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@ColumnFamily
public class ColumnFamilyBean implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private WideMap<Integer, String> map;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<Integer, String> getMap()
	{
		return map;
	}

	public void setMap(WideMap<Integer, String> map)
	{
		this.map = map;
	}
}
