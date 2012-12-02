package parser.entity;

import java.util.Date;

import javax.persistence.Id;

import fr.doan.achilles.entity.type.MultiKey;

/**
 * MyMultiKey
 * 
 * @author DuyHai DOAN
 * 
 */
public class MyMultiKey implements MultiKey
{

	@Id
	String name;

	@Id
	Integer rank;

	@Id
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
