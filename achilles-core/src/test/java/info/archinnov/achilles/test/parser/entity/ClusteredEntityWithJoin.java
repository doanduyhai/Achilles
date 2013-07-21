package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.test.mapping.entity.UserBean;
import javax.persistence.CascadeType;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;

/**
 * ClusteredEntityWithJoin
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class ClusteredEntityWithJoin
{

    @EmbeddedId
    private CompoundKey id;

    @JoinColumn
    @ManyToMany(cascade = CascadeType.ALL)
    private UserBean friend;

    public CompoundKey getId() {
        return id;
    }

    public void setId(CompoundKey id) {
        this.id = id;
    }

    public UserBean getFriend() {
        return friend;
    }

    public void setFriend(UserBean friends) {
        this.friend = friends;
    }

}
