package parser.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.entity.metadata.PropertyType;

@CompoundKey
public class CompoundKeyWithEnum {

    @Order(1)
    private Long id;

    @Order(2)
    private PropertyType type;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public PropertyType getType() {
        return type;
    }

    public void setType(PropertyType type) {
        this.type = type;
    }
}
