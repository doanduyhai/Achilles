package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.annotations.CompoundKey;


/**
 * MultiKeyWithNoAnnotation
 * 
 * @author DuyHai DOAN
 * 
 */
@CompoundKey
public class CompoundKeyWithNoAnnotation
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
