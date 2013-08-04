package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.type.Counter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

/**
 * CompleteBean
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class CompleteBean
{

    public static final long serialVersionUID = 151L;

    @Id
    private Long id;

    @Column
    private String name;

    @Lazy
    @Column
    private String label;

    @Column(name = "age_in_years")
    private Long age;

    @Lazy
    @Column
    private List<String> friends;

    @Column
    private Set<String> followers;

    @Column
    private Map<Integer, String> preferences;

    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn
    private Tweet welcomeTweet;

    @Column
    private Counter version;

    @OneToMany(cascade = CascadeType.ALL)
    @JoinColumn
    private List<Tweet> favoriteTweets;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getLabel()
    {
        return label;
    }

    public void setLabel(String label)
    {
        this.label = label;
    }

    public List<String> getFriends()
    {
        return friends;
    }

    public void setFriends(List<String> friends)
    {
        this.friends = friends;
    }

    public Set<String> getFollowers()
    {
        return followers;
    }

    public void setFollowers(Set<String> followers)
    {
        this.followers = followers;
    }

    public Map<Integer, String> getPreferences()
    {
        return preferences;
    }

    public void setPreferences(Map<Integer, String> preferences)
    {
        this.preferences = preferences;
    }

    public Long getAge()
    {
        return age;
    }

    public void setAge(Long age)
    {
        this.age = age;
    }

    public Tweet getWelcomeTweet()
    {
        return welcomeTweet;
    }

    public void setWelcomeTweet(Tweet welcomeTweet)
    {
        this.welcomeTweet = welcomeTweet;
    }

    public Counter getVersion()
    {
        return version;
    }

    public void setVersion(Counter version) {
        this.version = version;
    }

    public List<Tweet> getFavoriteTweets() {
        return favoriteTweets;
    }

    public void setFavoriteTweets(List<Tweet> favoriteTweets) {
        this.favoriteTweets = favoriteTweets;
    }

    @CompoundKey
    public static class UserTweetKey
    {
        @Order(1)
        private String user;

        @Order(2)
        private UUID tweet;

        public UserTweetKey() {
        }

        public UserTweetKey(String user, UUID tweet) {
            super();
            this.user = user;
            this.tweet = tweet;
        }

        public String getUser()
        {
            return user;
        }

        public void setUser(String user)
        {
            this.user = user;
        }

        public UUID getTweet()
        {
            return tweet;
        }

        public void setTweet(UUID tweet)
        {
            this.tweet = tweet;
        }

    }
}
