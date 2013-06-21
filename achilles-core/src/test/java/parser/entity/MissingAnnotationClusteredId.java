package parser.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.annotations.MultiKey;

import javax.persistence.Column;

@MultiKey
public class MissingAnnotationClusteredId
{

    @Key(order = 1)
    private Long userId;

    @Key(order = 2)
    @Column
    private String name;

    public MissingAnnotationClusteredId() {
    }

    public MissingAnnotationClusteredId(Long userId, String name) {
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
