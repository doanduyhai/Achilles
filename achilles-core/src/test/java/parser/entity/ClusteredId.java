package parser.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.annotations.MultiKey;
import javax.persistence.Column;

@MultiKey
public class ClusteredId
{

    @Key(order = 1)
    @Column(name = "id")
    private Long userId;

    @Key(order = 2)
    @Column
    private String name;

    public ClusteredId() {
    }

    public ClusteredId(Long userId, String name) {
        this.userId = userId;
        this.name = name;
    }

    public Long getUserId()
    {
        return userId;
    }

    public void setUserId(Long userId)
    {
        this.userId = userId;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }
}
