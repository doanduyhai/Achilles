package mapping.entity;

import fr.doan.achilles.annotations.Key;
import fr.doan.achilles.entity.type.MultiKey;

/**
 * WideRowMultiKey
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideRowMultiKey implements MultiKey
{
	@Key(order = 1)
	private Long index;

	@Key(order = 2)
	private String name;

	public WideRowMultiKey() {}

	public WideRowMultiKey(Long index, String name) {
		this.index = index;
		this.name = name;
	}

	public Long getIndex()
	{
		return index;
	}

	public void setIndex(Long index)
	{
		this.index = index;
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
