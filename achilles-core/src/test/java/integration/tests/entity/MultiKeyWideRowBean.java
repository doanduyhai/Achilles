package integration.tests.entity;

import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.type.WideMap;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * MultiKeyColumnFamilyBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@WideRow
public class MultiKeyWideRowBean implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private WideMap<WideRowMultiKey, String> map;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<WideRowMultiKey, String> getMap()
	{
		return map;
	}

}
