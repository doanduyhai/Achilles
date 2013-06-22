package integration.tests.entity;

import info.archinnov.achilles.annotations.MultiKey;
import info.archinnov.achilles.annotations.Order;

/**
 * CompoundKeyWithEnum
 * 
 * @author DuyHai DOAN
 * 
 */
@MultiKey
public class CompoundKeyWithEnum
{
    @Order(1)
    private Long index;

    @Order(2)
    private Type type;

    public CompoundKeyWithEnum() {
    }

    public CompoundKeyWithEnum(Long index, Type type) {
        this.index = index;
        this.type = type;
    }

    public Long getIndex()
    {
        return index;
    }

    public void setIndex(Long index)
    {
        this.index = index;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public static enum Type
    {
        AUDIO, IMAGE, FILE;
    }
}
