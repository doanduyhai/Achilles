package info.archinnov.achilles.interceptor;

import info.archinnov.achilles.entity.metadata.EntityMeta;

public class EventHolder {

    private EntityMeta meta;
    private Object entity;
    private Event event;

    public EventHolder(EntityMeta meta, Object entity, Event event) {
        this.meta = meta;
        this.entity = entity;
        this.event = event;
    }

    public void triggerInterception() {
        meta.intercept(entity,event);
    }

}
