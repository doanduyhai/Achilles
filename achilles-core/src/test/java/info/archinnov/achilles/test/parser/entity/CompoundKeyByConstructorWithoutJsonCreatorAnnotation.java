package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.annotations.Order;
import org.codehaus.jackson.annotate.JsonProperty;

public class CompoundKeyByConstructorWithoutJsonCreatorAnnotation {

    private Long id;

    private String name;

    public CompoundKeyByConstructorWithoutJsonCreatorAnnotation(@JsonProperty("id") @Order(1) Long id,
            @JsonProperty("name") @Order(2) String name) {
        this.name = name;
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

}
