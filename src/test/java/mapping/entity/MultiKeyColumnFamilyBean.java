package mapping.entity;

import info.archinnov.achilles.annotations.ColumnFamily;
import info.archinnov.achilles.entity.type.WideMap;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;


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
