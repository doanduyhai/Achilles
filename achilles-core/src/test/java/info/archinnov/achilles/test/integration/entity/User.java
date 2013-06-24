package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.type.WideMap;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.OneToOne;

/**
 * User
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class User {
    @Id
    private Long id;

    @Column
    private String firstname;

    @Column
    private String lastname;

    @ManyToMany(cascade = CascadeType.ALL)
    @JoinColumn(table = "user_tweets")
    private WideMap<Integer, Tweet> tweets;

    @ManyToMany(cascade = CascadeType.MERGE)
    @JoinColumn(table = "user_timeline")
    private WideMap<Long, Tweet> timeline;

    @ManyToMany(cascade = CascadeType.PERSIST)
    @JoinColumn(table = "retweets_cf")
    private WideMap<Integer, Tweet> retweets;

    @OneToOne(cascade = CascadeType.ALL)
    @JoinColumn
    private User referrer;

    public User() {
    }

    public User(Long id, String firstname, String lastname) {
        this.id = id;
        this.firstname = firstname;
        this.lastname = lastname;
    }

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

    public WideMap<Integer, Tweet> getTweets() {
        return tweets;
    }

    public WideMap<Long, Tweet> getTimeline() {
        return timeline;
    }

    public WideMap<Integer, Tweet> getRetweets() {
        return retweets;
    }

    public User getReferrer() {
        return referrer;
    }

    public void setReferrer(User referrer) {
        this.referrer = referrer;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((firstname == null) ? 0 : firstname.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((lastname == null) ? 0 : lastname.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        User other = (User) obj;
        if (firstname == null) {
            if (other.firstname != null)
                return false;
        } else if (!firstname.equals(other.firstname))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (lastname == null) {
            if (other.lastname != null)
                return false;
        } else if (!lastname.equals(other.lastname))
            return false;
        return true;
    }

}
