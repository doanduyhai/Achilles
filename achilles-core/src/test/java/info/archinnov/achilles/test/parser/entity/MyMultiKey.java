package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;
import java.util.Date;

/**
 * MyMultiKey
 * 
 * @author DuyHai DOAN
 * 
 */
@CompoundKey
public class MyMultiKey
{

    @Order(1)
    String name;

    @Order(2)
    Integer rank;

    @Order(3)
    Date creationDate;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Integer getRank()
    {
        return rank;
    }

    public void setRank(Integer rank)
    {
        this.rank = rank;
    }

    public Date getCreationDate()
    {
        return creationDate;
    }

    public void setCreationDate(Date creationDate)
    {
        this.creationDate = creationDate;
    }
}
