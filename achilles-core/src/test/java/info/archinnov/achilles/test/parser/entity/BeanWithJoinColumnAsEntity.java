package info.archinnov.achilles.test.parser.entity;

import java.util.Map;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

/**
 * BeanWithJoinColumnAsEntity
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithJoinColumnAsEntity
{
    @Id
    private Long id;

    @JoinColumn
    private Map<Integer, Bean> map;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public Map<Integer, Bean> getMap() {
        return map;
    }

    public void setMap(Map<Integer, Bean> map) {
        this.map = map;
    }

}
