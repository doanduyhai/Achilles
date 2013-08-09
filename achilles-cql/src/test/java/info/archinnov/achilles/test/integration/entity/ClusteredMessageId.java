package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Order;

public class ClusteredMessageId {

    @Order(1)
    private Long id;

    @Order(2)
    private Type type;

    public ClusteredMessageId() {
    }

    public ClusteredMessageId(Long id, Type type) {
        this.id = id;
        this.type = type;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public static enum Type
    {
        TEXT, AUDIO, FILE, IMAGE;
    }
}
