package parser.entity;

import javax.persistence.Id;

import fr.doan.achilles.entity.type.MultiKey;

/**
 * CorrectMultiKey
 * 
 * @author DuyHai DOAN
 * 
 */
public class CorrectMultiKey implements MultiKey
{
	@Id
	private String name;

	@Id
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
