package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Order;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * CompoundKeyWithEnum
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompoundKeyWithEnum
{

    private Long index;

    private Type type;

    @JsonCreator
    public CompoundKeyWithEnum(@Order(1) @JsonProperty("index") Long index,
            @Order(2) @JsonProperty("type") Type type) {
        this.index = index;
        this.type = type;
    }

    public Long getIndex() {
        return index;
    }

    public Type getType() {
        return type;
    }

    public static enum Type
    {
        AUDIO, IMAGE, FILE;
    }
}
