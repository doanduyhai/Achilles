package parser.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.annotations.Order;
import javax.persistence.Column;
import org.codehaus.jackson.annotate.JsonCreator;

@CompoundKey
public class CompoundKeyByConstructorWithoutOrderAnnotation {

    @Column(name = "primaryKey")
    private Long id;

    private String name;

    @JsonCreator
    public CompoundKeyByConstructorWithoutOrderAnnotation(@Order(1) String name,
            @Order(1) Long id) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

}
