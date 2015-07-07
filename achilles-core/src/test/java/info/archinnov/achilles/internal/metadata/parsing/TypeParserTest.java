package info.archinnov.achilles.internal.metadata.parsing;

import static org.fest.assertions.api.Assertions.*;

import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.utils.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class TypeParserTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void should_infer_value_class_from_list() throws Exception {
        @SuppressWarnings("unused")
        class Test {
            private List<String> friends;
        }

        Type type = Test.class.getDeclaredField("friends").getGenericType();

        Class<String> infered = TypeParser.inferValueClassForListOrSet(type, Test.class);

        assertThat(infered).isEqualTo(String.class);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void should_infer_parameterized_value_class_from_list() throws Exception {
        @SuppressWarnings("unused")
        class Test {
            private List<Class<Void>> friends;
        }

        Type type = Test.class.getDeclaredField("friends").getGenericType();

        Class infered = TypeParser.inferValueClassForListOrSet(type, Test.class);

        assertThat(infered).isEqualTo(Class.class);
    }


    @Test
    public void should_exception_when_infering_value_type_from_raw_list() throws Exception {
        @SuppressWarnings({ "rawtypes", "unused" })
        class Test {
            private List friends;
        }

        Type type = Test.class.getDeclaredField("friends").getGenericType();

        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("The type '" + type.getClass().getCanonicalName()
                + "' of the entity 'null' should be parameterized");

        TypeParser.inferValueClassForListOrSet(type, Test.class);
    }

    @Test
    public void should_infer_types_for_map() throws Exception {
        //Given
        @SuppressWarnings("unused")
        class Test {
            private Map<Integer, String> preferences;
        }

        Field mapField = Test.class.getDeclaredField("preferences");

        //When
        Pair<Class<Integer>, Class<String>> classes = TypeParser.determineMapGenericTypes(mapField);

        //Then
        assertThat(classes.left).isSameAs(Integer.class);
        assertThat(classes.right).isSameAs(String.class);
    }
}