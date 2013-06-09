package mapping.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.type.MultiKey;

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
	private Integer retweetCount;

	public TweetMultiKey() {}

	public TweetMultiKey(UUID id, String author, Integer retweetCount) {
		this.id = id;
		this.author = author;
		this.retweetCount = retweetCount;
	}

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

	public Integer getRetweetCount()
	{
		return retweetCount;
	}

	public void setRetweetCount(Integer retweetCount)
	{
		this.retweetCount = retweetCount;
	}

}
