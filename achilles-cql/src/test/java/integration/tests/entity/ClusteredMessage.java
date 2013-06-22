package integration.tests.entity;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

@Entity
public class ClusteredMessage {

    @EmbeddedId
    private ClusteredMessageId id;

    @Column
    private String label;

    public ClusteredMessage() {
    }

    public ClusteredMessage(ClusteredMessageId id, String label) {
        this.id = id;
        this.label = label;
    }

    public ClusteredMessageId getId() {
        return id;
    }

    public void setId(ClusteredMessageId id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
