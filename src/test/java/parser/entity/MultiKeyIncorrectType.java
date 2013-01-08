package parser.entity;

import java.util.List;

import fr.doan.achilles.annotations.Key;
import fr.doan.achilles.entity.type.MultiKey;

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
