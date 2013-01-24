package mapping.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import fr.doan.achilles.entity.type.WideMap;

/**
 * WideRowBean
 * 
 * @author DuyHai DOAN
 * 
 */
@fr.doan.achilles.annotations.WideRow
@Entity
public class WideRowBean implements Serializable
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
