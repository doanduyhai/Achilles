package parser.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;

/**
 * MultiKeyNotInstantiable
 * 
 * @author DuyHai DOAN
 * 
 */
@CompoundKey
public class CompoundKeyNotInstantiable
{
    @Order(1)
    private String name;

    @Order(2)
    private int rank;

    public CompoundKeyNotInstantiable(String name, int rank) {
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
