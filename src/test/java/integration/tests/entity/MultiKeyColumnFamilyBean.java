package integration.tests.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import fr.doan.achilles.annotations.ColumnFamily;
import fr.doan.achilles.entity.type.WideMap;

/**
 * MultiKeyWideRowBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@ColumnFamily
public class MultiKeyColumnFamilyBean implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Id
	private Long id;

	@Column
	private WideMap<ColumnFamilyMultiKey, String> map;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideMap<ColumnFamilyMultiKey, String> getMap()
	{
		return map;
	}

}
