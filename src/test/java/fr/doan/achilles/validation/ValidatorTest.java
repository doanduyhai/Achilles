package fr.doan.achilles.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import fr.doan.achilles.exception.ValidationException;

public class ValidatorTest {

    @Test(expected = ValidationException.class)
    public void should_exception_when_blank() throws Exception {
        Validator.validateNotBlank("", "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_string_null() throws Exception {
        Validator.validateNotBlank(null, "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_null() throws Exception {
        Validator.validateNotNull(null, "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_empty_collection() throws Exception {
        Validator.validateNotEmpty(new ArrayList<String>(), "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_null_collection() throws Exception {
        Validator.validateNotEmpty((Collection<String>) null, "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_incorrect_size_collection() throws Exception {
        Validator.validateSize(Arrays.asList("test"), 2, "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_empty_map() throws Exception {
        Validator.validateNotEmpty(new HashMap<String, String>(), "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_null_map() throws Exception {
        Validator.validateNotEmpty((Map<String, String>) null, "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_incorrect_size_amp() throws Exception {
        Validator.validateSize(new HashMap<String, String>(), 2, "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_no_default_constructor() throws Exception {
        Validator.validateNoargsConstructor(TestNoArgConstructor.class);
    }

    @Test
    public void should_match_pattern() throws Exception {
        Validator.validateRegExp("1_abcd01_sdf", "[a-zA-Z0-9_]+", "arg");
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_not_matching_pattern() throws Exception {
        Validator.validateRegExp("1_a-bcd01_sdf", "[a-zA-Z0-9_]+", "arg");
    }

    @Test
    public void should_instanciate_a_bean() throws Exception {
        Validator.validateInstantiable(NormalClass.class);
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_private_class() throws Exception {
        Validator.validateInstantiable(PrivateEntity.class);
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_interface() throws Exception {
        Validator.validateInstantiable(TestInterface.class);
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_abstract_class() throws Exception {
        Validator.validateInstantiable(AbstractClass.class);
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_primitive() throws Exception {
        Validator.validateInstantiable(Long.class);
    }

    @Test(expected = ValidationException.class)
    public void should_exception_array_type() throws Exception {
        String[] array = new String[2];
        Validator.validateInstantiable(array.getClass());
    }

    class TestNoArgConstructor {
        private TestNoArgConstructor() {
        }
    }
}
