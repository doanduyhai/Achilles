package parser.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.annotations.MultiKey;

import java.util.Date;

/**
 * MultiKeyWithDuplicateOrder
 * 
 * @author DuyHai DOAN
 * 
 */
@MultiKey
public class MultiKeyWithDuplicateOrder
{
    @Key(order = 1)
    private String name;

    @Key(order = 1)
    private int rank;

    @Key(order = 4)
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
