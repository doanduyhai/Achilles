package mapping.entity;

import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.annotations.MultiKey;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.WideMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;

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

    @Column
    private Map<Integer, UserBean> usersMap;

    @Column
    private WideMap<UUID, String> tweets;

    @Column(table = "user_tweets")
    private WideMap<UserTweetKey, String> userTweets;

    @Column(table = "geo_positions")
    private WideMap<UUID, String> geoPositions;

    @Column(table = "friend_widemap")
    private WideMap<String, UserBean> friendsWideMap;

    @ManyToOne
    @JoinColumn
    private UserBean user;

    @ManyToMany
    @JoinColumn(table = "join_users")
    private WideMap<Long, UserBean> joinUsers;

    @Column
    private Counter count;

    @Column(table = "popular_topics")
    private WideMap<String, Counter> popularTopics;

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

    public WideMap<UUID, String> getTweets()
    {
        return tweets;
    }

    public void setTweets(WideMap<UUID, String> tweets)
    {
        this.tweets = tweets;
    }

    public WideMap<UserTweetKey, String> getUserTweets()
    {
        return userTweets;
    }

    public void setUserTweets(WideMap<UserTweetKey, String> userTweets)
    {
        this.userTweets = userTweets;
    }

    public UserBean getUser()
    {
        return user;
    }

    public void setUser(UserBean user)
    {
        this.user = user;
    }

    public WideMap<UUID, String> getGeoPositions()
    {
        return geoPositions;
    }

    public void setGeoPositions(WideMap<UUID, String> geoPositions)
    {
        this.geoPositions = geoPositions;
    }

    public WideMap<Long, UserBean> getJoinUsers()
    {
        return joinUsers;
    }

    public Counter getCount()
    {
        return count;
    }

    public WideMap<String, Counter> getPopularTopics()
    {
        return popularTopics;
    }

    public Map<Integer, UserBean> getUsersMap()
    {
        return usersMap;
    }

    public void setUsersMap(Map<Integer, UserBean> usersMap)
    {
        this.usersMap = usersMap;
    }

    public WideMap<String, UserBean> getFriendsWideMap()
    {
        return friendsWideMap;
    }

    @MultiKey
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
