package integration.tests.entity;

import java.io.Serializable;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import fr.doan.achilles.entity.type.WideMap;

@Table
public class Tweet implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    private UUID id;

    @ManyToOne
    @JoinColumn
    private User creator;

    @Column
    private String content;

    @ManyToMany
    @JoinColumn
    private WideMap<Long, User> inTimelineOfUsers;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public User getCreator() {
        return creator;
    }

    public void setCreator(User creator) {
        this.creator = creator;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public WideMap<Long, User> getInTimelineOfUsers() {
        return inTimelineOfUsers;
    }

    public void setInTimelineOfUsers(WideMap<Long, User> inTimelineOfUsers) {
        this.inTimelineOfUsers = inTimelineOfUsers;
    }

}
