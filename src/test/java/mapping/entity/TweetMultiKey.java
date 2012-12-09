package mapping.entity;

import java.util.UUID;

import fr.doan.achilles.annotations.Key;
import fr.doan.achilles.entity.type.MultiKey;

/**
 * TweetMultiKey
 * 
 * @author DuyHai DOAN
 * 
 */
public class TweetMultiKey implements MultiKey
{
	@Key(order = 2)
	private String author;

	@Key(order = 1)
	private UUID id;

	@Key(order = 3)
	private int retweetCount;

	public UUID getId()
	{
		return id;
	}

	public void setId(UUID id)
	{
		this.id = id;
	}

	public String getAuthor()
	{
		return author;
	}

	public void setAuthor(String author)
	{
		this.author = author;
	}

	public int getRetweetCount()
	{
		return retweetCount;
	}

	public void setRetweetCount(int retweetCount)
	{
		this.retweetCount = retweetCount;
	}

}
