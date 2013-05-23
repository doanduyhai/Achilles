package parser.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.type.MultiKey;

/**
 * MultiKeyWithNegativeOrder
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWithNegativeOrder implements MultiKey
{
	@Key(order = -1)
	private String name;

	@Key(order = 0)
	private int rank;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public int getRank()
	{
		return rank;
	}

	public void setRank(int rank)
	{
		this.rank = rank;
	}

}
