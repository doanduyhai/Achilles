package integration.tests.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.type.MultiKey;

import java.util.Date;
import java.util.UUID;

import javax.persistence.Column;

/**
 * CompoundPrimaryKey
 * 
 * @author DuyHai DOAN
 * 
 */
public class ClusteredTweetId implements MultiKey
{
	@Key(order = 1)
	@Column(name = "user_id")
	private Long userId;

	@Key(order = 2)
	@Column(name = "tweet_id")
	private UUID tweetId;

	@Key(order = 3)
	@Column(name = "creation_date")
	private Date creationDate;

	public ClusteredTweetId() {}

	public ClusteredTweetId(Long userId, UUID tweetId) {
		this.userId = userId;
		this.tweetId = tweetId;
	}

	public ClusteredTweetId(Long userId, UUID tweetId, Date creationDate) {
		this.userId = userId;
		this.tweetId = tweetId;
		this.creationDate = creationDate;
	}

	public Long getUserId()
	{
		return userId;
	}

	public void setUserId(Long userId)
	{
		this.userId = userId;
	}

	public UUID getTweetId()
	{
		return tweetId;
	}

	public void setTweetId(UUID tweetId)
	{
		this.tweetId = tweetId;
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
