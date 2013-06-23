package parser.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.entity.metadata.PropertyType;

@CompoundKey
public class CompoundKeyByConstructorWithEnum {

    private Long id;

    private PropertyType type;

    public CompoundKeyByConstructorWithEnum(@Order(1) Long id, @Order(2) PropertyType type) {
        this.id = id;
        this.type = type;
    }

    public Long getId() {
        return id;
    }

    public PropertyType getType() {
        return type;
    }
}
