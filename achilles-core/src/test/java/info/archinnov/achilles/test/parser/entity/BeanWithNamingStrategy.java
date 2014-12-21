package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.type.NamingStrategy;

@Entity(keyspace = "myKeyspace", table = "myTable")
@Strategy(naming = NamingStrategy.SNAKE_CASE)
public class BeanWithNamingStrategy {

    @PartitionKey
    @Column(name = "my_Id")
    private Long id;

    @Column
    private String firstName;

    @Column(name = "\"lastName\"")
    private String lastName;

    private String unMappedColumn;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

}
