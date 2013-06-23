package parser.entity;

import info.archinnov.achilles.annotations.CompoundKey;
import javax.persistence.Column;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

@CompoundKey
public class CompoundKeyByConstructorWithoutJsonPropertyAnnotation {

    @Column(name = "primaryKey")
    private Long id;

    private String name;

    @JsonCreator
    public CompoundKeyByConstructorWithoutJsonPropertyAnnotation(@JsonProperty("name") String name,
            @JsonProperty("id") Long id) {
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
