package info.archinnov.achilles.entity.parsing;

import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.entity.metadata.CompoundKeyProperties;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;
import parser.entity.CompoundKeyByConstructor;
import parser.entity.CompoundKeyByConstructorMissingJsonPropertyAnnotation;
import parser.entity.CompoundKeyByConstructorMissingJsonPropertyAnnotationAttribute;
import parser.entity.CompoundKeyByConstructorMissingOrderAnnotation;
import parser.entity.CompoundKeyByConstructorWithDuplicateJsonPropertyNames;
import parser.entity.CompoundKeyByConstructorWithMissingField;
import parser.entity.CompoundKeyByConstructorWithoutJsonCreatorAnnotation;
import parser.entity.CompoundKeyByConstructorWithoutOrderAnnotation;
import parser.entity.CompoundKeyIncorrectType;
import parser.entity.CompoundKeyNotInstantiable;
import parser.entity.CompoundKeyWithDuplicateOrder;
import parser.entity.CompoundKeyWithNegativeOrder;
import parser.entity.CompoundKeyWithNoAnnotation;
import parser.entity.CompoundKeyWithOnlyOneComponent;
import parser.entity.CorrectCompoundKey;

@RunWith(MockitoJUnitRunner.class)
public class CompoundKeyParserTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private CompoundKeyParser parser;

    @Test
    public void should_parse_compound_key() throws Exception
    {
        Method nameGetter = CorrectCompoundKey.class.getMethod("getName");
        Method nameSetter = CorrectCompoundKey.class.getMethod("setName", String.class);

        Method rankGetter = CorrectCompoundKey.class.getMethod("getRank");
        Method rankSetter = CorrectCompoundKey.class.getMethod("setRank", int.class);
        Constructor<?> constructor = CorrectCompoundKey.class.getConstructor();

        CompoundKeyProperties props = parser.parseCompoundKey(CorrectCompoundKey.class);

        assertThat((Constructor) props.getConstructor()).isEqualTo(constructor);
        assertThat(props.getComponentGetters()).containsExactly(nameGetter, rankGetter);
        assertThat(props.getComponentSetters()).containsExactly(nameSetter, rankSetter);
        assertThat(props.getComponentClasses()).containsExactly(String.class, int.class);
        assertThat(props.getComponentNames()).containsExactly("name", "rank");
        assertThat(props.getCQLComponentNames()).containsExactly("name", "rank");
        assertThat(props.getCQLOrderingComponent()).isEqualTo("rank");
    }

    @Test
    public void should_parse_compound_key_constructor_injection() throws Exception {

        Method idGetter = CompoundKeyByConstructor.class.getMethod("getId");
        Method nameGetter = CompoundKeyByConstructor.class.getMethod("getName");

        Constructor<?> constructor = CompoundKeyByConstructor.class.getConstructor(Long.class, String.class);

        CompoundKeyProperties props = parser.parseCompoundKey(CompoundKeyByConstructor.class);

        assertThat((Constructor) props.getConstructor()).isEqualTo(constructor);
        assertThat(props.getComponentClasses()).containsExactly(Long.class, String.class);
        assertThat(props.getComponentGetters()).containsExactly(idGetter, nameGetter);
        assertThat(props.getComponentSetters()).isEmpty();
        assertThat(props.getComponentNames()).containsExactly("primaryKey", "name");
        assertThat(props.getCQLComponentNames()).containsExactly("primarykey", "name");
        assertThat(props.getCQLOrderingComponent()).isEqualTo("name");
    }

    @Test
    public void should_exception_when_compound_key_incorrect_type() throws Exception
    {
        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("The class 'java.util.List' is not a valid component type for the @CompoundKey class '"
                        + CompoundKeyIncorrectType.class.getCanonicalName() + "'");

        parser.parseCompoundKey(CompoundKeyIncorrectType.class);
    }

    @Test
    public void should_exception_when_compound_key_wrong_key_order() throws Exception
    {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The key orders is wrong for @CompoundKey class '"
                + CompoundKeyWithNegativeOrder.class.getCanonicalName() + "'");

        parser.parseCompoundKey(CompoundKeyWithNegativeOrder.class);
    }

    @Test
    public void should_exception_when_compound_key_has_no_annotation() throws Exception
    {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("There should be exactly one constructor for @CompoundKey class '"
                + CompoundKeyWithNoAnnotation.class.getCanonicalName()
                + "' annotated by @JsonCreator and all arguments annotated by @Order AND @JsonProperty");

        parser.parseCompoundKey(CompoundKeyWithNoAnnotation.class);
    }

    @Test
    public void should_exception_when_compound_key_has_duplicate_order() throws Exception
    {
        exception.expect(AchillesBeanMappingException.class);

        exception.expectMessage("The order '1' is duplicated in @CompoundKey class '"
                + CompoundKeyWithDuplicateOrder.class.getCanonicalName() + "'");

        parser.parseCompoundKey(CompoundKeyWithDuplicateOrder.class);
    }

    @Test
    public void should_exception_when_compound_key_no_pulic_default_constructor() throws Exception
    {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The @CompoundKey class '" + CompoundKeyNotInstantiable.class.getCanonicalName()
                + "' should have a public default constructor");

        parser.parseCompoundKey(CompoundKeyNotInstantiable.class);
    }

    @Test
    public void should_exception_when_compound_key_by_constructor_no_order_annotation() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("There should be exactly one constructor for @CompoundKey class '"
                + CompoundKeyByConstructorWithoutOrderAnnotation.class.getCanonicalName()
                + "' annotated by @JsonCreator and all arguments annotated by @Order AND @JsonProperty");
        parser.parseCompoundKey(CompoundKeyByConstructorWithoutOrderAnnotation.class);
    }

    @Test
    public void should_exception_when_compound_key_by_constructor_no_json_property_annotation() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("There should be exactly one constructor for @CompoundKey class '"
                + CompoundKeyByConstructorWithoutOrderAnnotation.class.getCanonicalName()
                + "' annotated by @JsonCreator and all arguments annotated by @Order AND @JsonProperty");
        parser.parseCompoundKey(CompoundKeyByConstructorWithoutOrderAnnotation.class);
    }

    @Test
    public void should_exception_when_compound_key_by_constructor_no_json_creator_annotation() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("There should be exactly one constructor for @CompoundKey class '"
                + CompoundKeyByConstructorWithoutJsonCreatorAnnotation.class.getCanonicalName()
                + "' annotated by @JsonCreator and all arguments annotated by @Order AND @JsonProperty");
        parser.parseCompoundKey(CompoundKeyByConstructorWithoutJsonCreatorAnnotation.class);
    }

    @Test
    public void should_exception_when_compound_key_by_constructor_duplicated_json_property_name() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("The property names defined by @JsonProperty should be unique for the @CompoundKey class '"
                        + CompoundKeyByConstructorWithDuplicateJsonPropertyNames.class.getCanonicalName()
                        + "'");
        parser.parseCompoundKey(CompoundKeyByConstructorWithDuplicateJsonPropertyNames.class);
    }

    @Test
    public void should_exception_when_compound_key_by_constructor_missing_order_annotation() throws Exception {
        Constructor<?> constructor = CompoundKeyByConstructorMissingOrderAnnotation.class.getConstructor(
                String.class, Long.class);
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The constructor '" + constructor + "' of @CompoundKey class '"
                + CompoundKeyByConstructorMissingOrderAnnotation.class.getCanonicalName()
                + "' should have all its params annotated with @Order AND @JsonProperty");
        parser.parseCompoundKey(CompoundKeyByConstructorMissingOrderAnnotation.class);
    }

    @Test
    public void should_exception_when_compound_key_by_constructor_missing_json_property_annotation() throws Exception {
        Constructor<?> constructor = CompoundKeyByConstructorMissingJsonPropertyAnnotation.class.getConstructor(
                String.class, Long.class);
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The constructor '" + constructor + "' of @CompoundKey class '"
                + CompoundKeyByConstructorMissingJsonPropertyAnnotation.class.getCanonicalName()
                + "' should have all its params annotated with @Order AND @JsonProperty");
        parser.parseCompoundKey(CompoundKeyByConstructorMissingJsonPropertyAnnotation.class);
    }

    @Test
    public void should_exception_when_compound_key_by_constructor_missing_json_property_annotation_attribute()
            throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("@JsonProperty on constructor param should have a 'value' attribute for deserialization");
        parser.parseCompoundKey(CompoundKeyByConstructorMissingJsonPropertyAnnotationAttribute.class);
    }

    @Test
    public void should_exception_when_compound_key_has_only_one_component()
            throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("There should be at least 2 components for the @CompoundKey class '"
                        + CompoundKeyWithOnlyOneComponent.class.getCanonicalName()
                        + "'");
        parser.parseCompoundKey(CompoundKeyWithOnlyOneComponent.class);
    }

    @Test
    public void should_exception_when_compound_by_constructor_missing_field()
            throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("Cannot find field of type 'java.lang.String' and name 'name' in the @CompoundKey class "
                        + CompoundKeyByConstructorWithMissingField.class.getCanonicalName());
        parser.parseCompoundKey(CompoundKeyByConstructorWithMissingField.class);
    }
}
