package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Order;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * ClusteredEntityWithObjectValue
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Table(name = "clustered_with_object_value")
public class ClusteredEntityWithObjectValue {

    @EmbeddedId
    private ClusteredKey id;

    @Column
    private Holder value;

    public ClusteredEntityWithObjectValue() {
    }

    public ClusteredEntityWithObjectValue(ClusteredKey id, Holder value) {
        this.id = id;
        this.value = value;
    }

    public ClusteredKey getId() {
        return id;
    }

    public void setId(ClusteredKey id) {
        this.id = id;
    }

    public Holder getValue() {
        return value;
    }

    public void setValue(Holder value) {
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
        ClusteredEntityWithObjectValue other = (ClusteredEntityWithObjectValue) obj;
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
        private String name;

        public ClusteredKey() {
        }

        public ClusteredKey(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
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
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }

    }

    public static class Holder {

        private String content;

        public Holder() {
        }

        public Holder(String content) {
            this.content = content;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String name) {
            this.content = name;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((content == null) ? 0 : content.hashCode());
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
            Holder other = (Holder) obj;
            if (content == null) {
                if (other.content != null)
                    return false;
            } else if (!content.equals(other.content))
                return false;
            return true;
        }
    }
}
