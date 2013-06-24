package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.type.Counter;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * ClusteredEntityWithCounter
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Table(name = "clustered_with_counter_value")
public class ClusteredEntityWithCounter
{

    @EmbeddedId
    private ClusteredKey id;

    @Column
    private Counter counter;

    public ClusteredEntityWithCounter() {
    }

    public ClusteredEntityWithCounter(ClusteredKey id, Counter counter) {
        this.id = id;
        this.counter = counter;
    }

    public ClusteredKey getId()
    {
        return id;
    }

    public void setId(ClusteredKey id)
    {
        this.id = id;
    }

    public Counter getCounter()
    {
        return counter;
    }

    public void setCounter(Counter counter)
    {
        this.counter = counter;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        ClusteredEntityWithCounter other = (ClusteredEntityWithCounter) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    @CompoundKey
    public static class ClusteredKey
    {
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

        public Long getId()
        {
            return id;
        }

        public void setId(Long id)
        {
            this.id = id;
        }

        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
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
}
