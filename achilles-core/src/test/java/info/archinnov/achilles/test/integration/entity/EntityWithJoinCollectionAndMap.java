package info.archinnov.achilles.test.integration.entity;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;

/**
 * EntityWithJoinCollectionAndMap
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class EntityWithJoinCollectionAndMap
{

    @Id
    private Long id;

    @JoinColumn
    @OneToMany(cascade = CascadeType.PERSIST)
    private List<Tweet> tweets;

    @JoinColumn
    @ManyToMany
    private Set<User> friends;

    @JoinColumn
    @OneToMany(cascade = CascadeType.ALL)
    private Map<Integer, Tweet> timeline;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public List<Tweet> getTweets()
    {
        return tweets;
    }

    public void setTweets(List<Tweet> tweets)
    {
        this.tweets = tweets;
    }

    public Set<User> getFriends()
    {
        return friends;
    }

    public void setFriends(Set<User> friends)
    {
        this.friends = friends;
    }

    public Map<Integer, Tweet> getTimeline()
    {
        return timeline;
    }

    public void setTimeline(Map<Integer, Tweet> timeline)
    {
        this.timeline = timeline;
    }
}
