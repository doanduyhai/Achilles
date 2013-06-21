package parser.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.annotations.MultiKey;

/**
 * CorrectMultiKey
 * 
 * @author DuyHai DOAN
 * 
 */
@MultiKey
public class CorrectMultiKey
{
    @Key(order = 1)
    private String name;

    @Key(order = 2)
    private int rank;

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
