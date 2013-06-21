package parser.entity;

import info.archinnov.achilles.annotations.MultiKey;
import info.archinnov.achilles.annotations.Order;

/**
 * CorrectMultiKeyUnorderedKeys
 * 
 * @author DuyHai DOAN
 * 
 */
@MultiKey
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
