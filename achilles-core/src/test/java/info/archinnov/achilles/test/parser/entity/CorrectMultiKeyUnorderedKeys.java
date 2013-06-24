package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;

/**
 * CorrectMultiKeyUnorderedKeys
 * 
 * @author DuyHai DOAN
 * 
 */
@CompoundKey
public class CorrectMultiKeyUnorderedKeys
{
    @Order(2)
    private int rank;

    @Order(1)
    private String name;

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
