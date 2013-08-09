package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.annotations.Order;
import javax.persistence.Column;

public class CompoundKey
{

    @Order(1)
    @Column(name = "id")
    private Long userId;

    @Order(2)
    @Column
    private String name;

    public CompoundKey() {
    }

    public CompoundKey(Long userId, String name) {
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
