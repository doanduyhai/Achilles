package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.annotations.Strategy;
import info.archinnov.achilles.type.NamingStrategy;

@Entity(table = "snakeCaseNaming")
@Strategy(naming = NamingStrategy.SNAKE_CASE)
public class EntityWithNamingStrategy {

    @Id(name = "my_id")
    private Long id;

    @Column(name = "fn")
    private String firstName;

    @Column
    private String lastName;

    @Column(name = "\"nickName\"")
    private String nickName;

    public EntityWithNamingStrategy() {
    }

    public EntityWithNamingStrategy(Long id, String firstName, String lastName, String nickName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.nickName = nickName;
    }

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

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }
}
