package info.archinnov.achilles.internals.entities;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.PartitionKey;

@Entity(table = EntityWithNonExistingTable.TABLE)
public class EntityWithNonExistingTable {

    public static final String TABLE = "non_existing_table";

    @PartitionKey
    private Long id;

    @Column
    private String value;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
