package parser.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;
import java.util.Date;

/**
 * MultiKeyWithDuplicateOrder
 * 
 * @author DuyHai DOAN
 * 
 */
@CompoundKey
public class CompoundKeyWithDuplicateOrder
{
    @Order(1)
    private String name;

    @Order(1)
    private int rank;

    @Order(4)
    private Date date;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public int getRank()
    {
        return rank;
    }

    public void setRank(int rank)
    {
        this.rank = rank;
    }

    public Date getDate()
    {
        return date;
    }

    public void setDate(Date date)
    {
        this.date = date;
    }
}
