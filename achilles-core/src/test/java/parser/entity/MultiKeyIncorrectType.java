package parser.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.type.MultiKey;

import java.util.List;


/**
 * MultiKeyIncorrectType
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyIncorrectType implements MultiKey
{
	@Key(order = 1)
	private List<String> name;

	@Key(order = 2)
	private int rank;

	public List<String> getName()
	{
		return name;
	}

	public void setName(List<String> name)
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
