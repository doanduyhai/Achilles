package parser.entity;

import fr.doan.achilles.entity.type.MultiKey;

/**
 * MultiKeyWithNoAnnotation
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWithNoAnnotation implements MultiKey
{
	private Void name;

	private int rank;

	public Void getName()
	{
		return name;
	}

	public void setName(Void name)
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
