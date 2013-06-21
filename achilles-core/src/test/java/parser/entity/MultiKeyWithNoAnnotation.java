package parser.entity;

import info.archinnov.achilles.annotations.MultiKey;


/**
 * MultiKeyWithNoAnnotation
 * 
 * @author DuyHai DOAN
 * 
 */
@MultiKey
public class MultiKeyWithNoAnnotation
{
    private String name;

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
