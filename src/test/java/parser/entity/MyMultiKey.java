package parser.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.type.MultiKey;

import java.util.Date;


/**
 * MyMultiKey
 * 
 * @author DuyHai DOAN
 * 
 */
public class MyMultiKey implements MultiKey
{

	@Key(order = 1)
	String name;

	@Key(order = 2)
	Integer rank;

	@Key(order = 3)
	Date creationDate;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public Integer getRank()
	{
		return rank;
	}

	public void setRank(Integer rank)
	{
		this.rank = rank;
	}

	public Date getCreationDate()
	{
		return creationDate;
	}

	public void setCreationDate(Date creationDate)
	{
		this.creationDate = creationDate;
	}
}
