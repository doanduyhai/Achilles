package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.type.Counter;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

/**
 * Tweet
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class Tweet
{
    @Id
    private UUID id;

    @ManyToOne
    @JoinColumn
    private User creator;

    @Column
    private String content;

    @Column
    private Counter favoriteCount;

    public UUID getId()
    {
        return id;
    }

    public void setId(UUID id)
    {
        this.id = id;
    }

    public User getCreator()
    {
        return creator;
    }

    public void setCreator(User creator)
    {
        this.creator = creator;
    }

    public String getContent()
    {
        return content;
    }

    public void setContent(String content)
    {
        this.content = content;
    }

    public Counter getFavoriteCount()
    {
        return favoriteCount;
    }
}
