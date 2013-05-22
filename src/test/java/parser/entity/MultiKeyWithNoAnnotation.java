package parser.entity;

import info.archinnov.achilles.type.MultiKey;

/**
 * MultiKeyWithNoAnnotation
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWithNoAnnotation implements MultiKey
{
	private String name;

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
