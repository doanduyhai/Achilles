package integration.tests.entity;

import java.io.Serializable;
import java.util.UUID;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import fr.doan.achilles.entity.type.WideMap;

@Table
public class User implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    private Long id;

    @Column
    private String firstname;

    @Column
    private String lastname;

    @ManyToMany
    @JoinColumn
    private WideMap<Long, User> friends;

    @ManyToMany(cascade = CascadeType.ALL)
    @JoinColumn
    private WideMap<UUID, Tweet> tweets;

    @ManyToMany(cascade = CascadeType.MERGE)
    @JoinColumn
    private WideMap<UUID, Tweet> timeline;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public WideMap<Long, User> getFriends() {
        return friends;
    }

    public void setFriends(WideMap<Long, User> friends) {
        this.friends = friends;
    }

    public WideMap<UUID, Tweet> getTweets() {
        return tweets;
    }

    public void setTweets(WideMap<UUID, Tweet> tweets) {
        this.tweets = tweets;
    }

    public WideMap<UUID, Tweet> getTimeline() {
        return timeline;
    }

    public void setTimeline(WideMap<UUID, Tweet> timeline) {
        this.timeline = timeline;
    }

}
