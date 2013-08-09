package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Order;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * ClusteredEntity
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
@Table(name = "clustered")
public class ClusteredEntity
{
    @EmbeddedId
    private ClusteredKey id;

    @Column
    private String value;

    public ClusteredEntity() {
    }

    public ClusteredEntity(ClusteredKey id, String value) {
        this.id = id;
        this.value = value;
    }

    public ClusteredKey getId()
    {
        return id;
    }

    public void setId(ClusteredKey id)
    {
        this.id = id;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClusteredEntity other = (ClusteredEntity) obj;
        if (id == null)
        {
            if (other.id != null)
                return false;
        }
        else if (!id.equals(other.id))
            return false;
        if (value == null)
        {
            if (other.value != null)
                return false;
        }
        else if (!value.equals(other.value))
            return false;
        return true;
    }

    public static class ClusteredKey
    {
        @Column
        @Order(1)
        private Long id;

        @Column
        @Order(2)
        private Integer count;

        @Column
        @Order(3)
        private String name;

        public ClusteredKey() {
        }

        public ClusteredKey(Long id, Integer count, String name) {
            this.id = id;
            this.count = count;
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

        public Integer getCount()
        {
            return count;
        }

        public void setCount(Integer count)
        {
            this.count = count;
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
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((count == null) ? 0 : count.hashCode());
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ClusteredKey other = (ClusteredKey) obj;
            if (count == null)
            {
                if (other.count != null)
                    return false;
            }
            else if (!count.equals(other.count))
                return false;
            if (id == null)
            {
                if (other.id != null)
                    return false;
            }
            else if (!id.equals(other.id))
                return false;
            if (name == null)
            {
                if (other.name != null)
                    return false;
            }
            else if (!name.equals(other.name))
                return false;
            return true;
        }

    }

}
