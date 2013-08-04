package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.type.Counter;
import javax.persistence.Column;
import javax.persistence.Id;

/**
 * BeanWithSimpleCounter
 * 
 * @author DuyHai DOAN
 * 
 */
public class BeanWithSimpleCounter
{

    @Id
    private Long id;

    @Column
    private Counter counter;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public Counter getCounter()
    {
        return counter;
    }

    public void setCounter(Counter counter)
    {
        this.counter = counter;
    }

}
