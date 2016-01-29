package info.archinnov.achilles.internals.entities;

import java.nio.ByteBuffer;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.PartitionKey;

@Entity(table = EntityWithByteBufferType.TABLE)
public class EntityWithByteBufferType {

    public static final String TABLE = "table_with_bytebuffer";

    @PartitionKey
    private Long id;

    @Column
    private ByteBuffer value;

    public EntityWithByteBufferType() {
    }

    public EntityWithByteBufferType(Long id, ByteBuffer value) {
        this.id = id;
        this.value = value;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public void setValue(ByteBuffer value) {
        this.value = value;
    }
}
