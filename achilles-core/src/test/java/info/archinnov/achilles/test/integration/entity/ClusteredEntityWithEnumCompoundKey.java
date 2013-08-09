package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Order;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * ClusteredEntityWithEnumCompoundKey
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Table(name = "clustered_with_enum_compound")
public class ClusteredEntityWithEnumCompoundKey {
    @EmbeddedId
    private ClusteredKey id;

    @Column
    private String value;

    public ClusteredKey getId() {
        return id;
    }

    public void setId(ClusteredKey id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public ClusteredEntityWithEnumCompoundKey() {
    }

    public ClusteredEntityWithEnumCompoundKey(ClusteredKey id, String value) {
        super();
        this.id = id;
        this.value = value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClusteredEntityWithEnumCompoundKey other = (ClusteredEntityWithEnumCompoundKey) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

    public static class ClusteredKey {
        @Column
        @Order(1)
        private Long id;

        @Column
        @Order(2)
        private Type type;

        public ClusteredKey() {
        }

        public ClusteredKey(Long id, Type type) {
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

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ClusteredKey other = (ClusteredKey) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (type != other.type)
                return false;
            return true;
        }

    }

    public static enum Type {
        AUDIO, FILE, IMAGE;
    }
}
