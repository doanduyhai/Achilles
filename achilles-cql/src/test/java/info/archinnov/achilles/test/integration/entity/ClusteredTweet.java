package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.type.Counter;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

/**
 * ClusteredTweet
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class ClusteredTweet
{
    @EmbeddedId
    private ClusteredTweetId id;

    @Column
    private String content;

    @Column(name = "original_author_id")
    private Long originalAuthorId;

    @Column(name = "is_a_retweet")
    private Boolean isARetweet;

    @Column
    private Counter retweetCount;

    @Column
    private Counter favoriteCount;

    public ClusteredTweet() {
    }

    public ClusteredTweet(ClusteredTweetId id, String content, Long originalAuthorId,
            Boolean isARetweet)
    {
        this.id = id;
        this.content = content;
        this.originalAuthorId = originalAuthorId;
        this.isARetweet = isARetweet;
    }

    public ClusteredTweetId getId()
    {
        return id;
    }

    public void setId(ClusteredTweetId id)
    {
        this.id = id;
    }

    public String getContent()
    {
        return content;
    }

    public void setContent(String content)
    {
        this.content = content;
    }

    public Long getOriginalAuthorId()
    {
        return originalAuthorId;
    }

    public void setOriginalAuthorId(Long originalAuthorId)
    {
        this.originalAuthorId = originalAuthorId;
    }

    public Boolean getIsARetweet()
    {
        return isARetweet;
    }

    public void setIsARetweet(Boolean isARetweet)
    {
        this.isARetweet = isARetweet;
    }

    public Counter getRetweetCount()
    {
        return retweetCount;
    }

    public Counter getFavoriteCount()
    {
        return favoriteCount;
    }

    public void setRetweetCount(Counter retweetCount) {
        this.retweetCount = retweetCount;
    }

    public void setFavoriteCount(Counter favoriteCount) {
        this.favoriteCount = favoriteCount;
    }
}
