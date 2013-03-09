package parser.entity;

import info.archinnov.achilles.annotations.Counter;
import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Id;

/**
 * BeanWithSimpleCounter
 * 
 * @author DuyHai DOAN
 * 
 */
public class BeanWithSimpleCounter implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    private Long id;

    @Counter
    @Column
    private long counter;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }
}
