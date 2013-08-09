package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Order;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

/**
 * ClusteredEntityWithJoinEntity
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Table(name = "clustered_with_join_value")
public class ClusteredEntityWithJoinEntity {
    @EmbeddedId
    private ClusteredKey id;

    @JoinColumn
    @ManyToMany(cascade = CascadeType.ALL)
    private User friend;

    public ClusteredEntityWithJoinEntity() {
    }

    public ClusteredEntityWithJoinEntity(ClusteredKey id, User friend) {
        super();
        this.id = id;
        this.friend = friend;
    }

    public ClusteredKey getId() {
        return id;
    }

    public void setId(ClusteredKey id) {
        this.id = id;
    }

    public User getFriend() {
        return friend;
    }

    public void setFriend(User friend) {
        this.friend = friend;
    }

    public static class ClusteredKey {
        @Column
        @Order(1)
        private Long id;

        @Column
        @Order(2)
        private String name;

        public ClusteredKey() {
        }

        public ClusteredKey(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

    }

}
