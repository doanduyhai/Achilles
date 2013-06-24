package info.archinnov.achilles.test.builders;

import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.entity.User;

import java.util.UUID;

import org.apache.commons.lang.math.RandomUtils;

/**
 * TweetTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class TweetTestBuilder
{

	private UUID id;

	private User creator;

	private String content;

	public static TweetTestBuilder tweet()
	{
		return new TweetTestBuilder();
	}

	public Tweet buid()
	{
		Tweet tweet = new Tweet();

		tweet.setId(id);
		tweet.setCreator(creator);
		tweet.setContent(content);
		return tweet;
	}

	public TweetTestBuilder id(UUID id)
	{
		this.id = id;
		return this;
	}

	public TweetTestBuilder randomId()
	{
		this.id = new UUID(RandomUtils.nextLong(), RandomUtils.nextLong());
		return this;
	}

	public TweetTestBuilder content(String content)
	{
		this.content = content;
		return this;
	}

	public TweetTestBuilder creator(User creator)
	{
		this.creator = creator;
		return this;
	}
}
