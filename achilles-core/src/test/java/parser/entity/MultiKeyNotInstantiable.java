package parser.entity;

import info.archinnov.achilles.annotations.MultiKey;
import info.archinnov.achilles.annotations.Order;

/**
 * MultiKeyNotInstantiable
 * 
 * @author DuyHai DOAN
 * 
 */
@MultiKey
public class MultiKeyNotInstantiable
{
    @Order(1)
    private String name;

    @Order(2)
    private int rank;

    public MultiKeyNotInstantiable(String name, int rank) {
        super();
        this.name = name;
        this.rank = rank;
    }

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

}
