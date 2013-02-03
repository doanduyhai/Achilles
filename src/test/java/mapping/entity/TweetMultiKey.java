package mapping.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.entity.type.MultiKey;

import java.util.UUID;


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
