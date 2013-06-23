package mapping.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;
import java.util.UUID;

/**
 * TweetMultiKey
 * 
 * @author DuyHai DOAN
 * 
 */
@CompoundKey
public class TweetCompoundKey
{
    @Order(2)
    private String author;

    @Order(1)
    private UUID id;

    @Order(3)
    private Integer retweetCount;

    public TweetCompoundKey() {
    }

    public TweetCompoundKey(UUID id, String author, Integer retweetCount) {
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
