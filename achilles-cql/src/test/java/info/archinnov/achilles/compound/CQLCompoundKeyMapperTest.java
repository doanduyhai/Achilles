package info.archinnov.achilles.compound;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.proxy.MethodInvoker;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import parser.entity.CompoundKey;
import parser.entity.CompoundKeyByConstructor;
import parser.entity.CompoundKeyByConstructorWithEnum;
import parser.entity.CompoundKeyWithEnum;
import com.datastax.driver.core.Row;

@RunWith(MockitoJUnitRunner.class)
public class CQLCompoundKeyMapperTest {

    @InjectMocks
    private CQLCompoundKeyMapper mapper;

    @Mock
    private CQLRowMethodInvoker cqlRowInvoker;

    @Mock
    private MethodInvoker invoker;

    @Mock
    private PropertyMeta<?, ?> pm;

    @Mock
    private Row row;

    @Test
    public void should_write_component_to_compound_key_by_setter() throws Exception {
        Constructor<CompoundKey> constructor = CompoundKey.class.getConstructor();
        Method idSetter = CompoundKey.class.getDeclaredMethod("setUserId", Long.class);
        Method nameSetter = CompoundKey.class.getDeclaredMethod("setName", String.class);

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(true);
        when(pm.<CompoundKey> getCompoundKeyConstructor()).thenReturn(constructor);
        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("userid", "name"));
        when(pm.getComponentClasses()).thenReturn(Arrays.<Class<?>> asList(Long.class, String.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.asList(idSetter, nameSetter));

        when(row.isNull("userid")).thenReturn(false);
        when(row.isNull("name")).thenReturn(false);

        when(cqlRowInvoker.invokeOnRowForProperty(row, "userid", Long.class)).thenReturn(11L);
        when(cqlRowInvoker.invokeOnRowForProperty(row, "name", String.class)).thenReturn("DuyHai");

        Object actual = mapper.createFromRow(row, pm);

        assertThat(actual).isNotNull();
        assertThat(actual).isInstanceOf(CompoundKey.class);

        verify(invoker).setValueToField(actual, idSetter, 11L);
        verify(invoker).setValueToField(actual, nameSetter, "DuyHai");
    }

    @Test
    public void should_write_component_to_compound_with_enum_key_by_setter() throws Exception {
        Constructor<CompoundKeyWithEnum> constructor = CompoundKeyWithEnum.class.getConstructor();
        Method idSetter = CompoundKeyWithEnum.class.getDeclaredMethod("setId", Long.class);
        Method typeSetter = CompoundKeyWithEnum.class.getDeclaredMethod("setType", PropertyType.class);

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(true);
        when(pm.<CompoundKeyWithEnum> getCompoundKeyConstructor()).thenReturn(constructor);
        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("id", "type"));
        when(pm.getComponentClasses()).thenReturn(Arrays.<Class<?>> asList(Long.class, PropertyType.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.asList(idSetter, typeSetter));

        when(row.isNull("id")).thenReturn(false);
        when(row.isNull("type")).thenReturn(false);

        when(cqlRowInvoker.invokeOnRowForProperty(row, "id", Long.class)).thenReturn(11L);
        when(cqlRowInvoker.invokeOnRowForProperty(row, "type", String.class)).thenReturn("COMPOUND_ID");

        Object actual = mapper.createFromRow(row, pm);

        assertThat(actual).isNotNull();
        assertThat(actual).isInstanceOf(CompoundKeyWithEnum.class);

        verify(invoker).setValueToField(actual, idSetter, 11L);
        verify(invoker).setValueToField(actual, typeSetter, COMPOUND_ID);
    }

    @Test
    public void should_write_component_to_compound_key_by_constructor() throws Exception {
        Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class.getConstructor(Long.class,
                String.class);

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(false);
        when(pm.<CompoundKeyByConstructor> getCompoundKeyConstructor()).thenReturn(constructor);
        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("primaryKey", "name"));
        when(pm.getComponentClasses()).thenReturn(Arrays.<Class<?>> asList(Long.class, String.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.<Method> asList());

        when(row.isNull("primaryKey")).thenReturn(false);
        when(row.isNull("name")).thenReturn(false);

        when(cqlRowInvoker.invokeOnRowForProperty(row, "primaryKey", Long.class)).thenReturn(11L);
        when(cqlRowInvoker.invokeOnRowForProperty(row, "name", String.class)).thenReturn("DuyHai");

        Object actual = mapper.createFromRow(row, pm);

        assertThat(actual).isNotNull();
        assertThat(actual).isInstanceOf(CompoundKeyByConstructor.class);

        CompoundKeyByConstructor compoundKey = (CompoundKeyByConstructor) actual;

        assertThat(compoundKey.getId()).isEqualTo(11L);
        assertThat(compoundKey.getName()).isEqualTo("DuyHai");
    }

    @Test
    public void should_write_component_to_compound_key_with_enum_by_constructor() throws Exception {
        Constructor<CompoundKeyByConstructorWithEnum> constructor = CompoundKeyByConstructorWithEnum.class
                .getConstructor(Long.class,
                        PropertyType.class);

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(false);
        when(pm.<CompoundKeyByConstructorWithEnum> getCompoundKeyConstructor()).thenReturn(constructor);
        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("id", "name"));
        when(pm.getComponentClasses()).thenReturn(Arrays.<Class<?>> asList(Long.class, PropertyType.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.<Method> asList());

        when(row.isNull("id")).thenReturn(false);
        when(row.isNull("name")).thenReturn(false);

        when(cqlRowInvoker.invokeOnRowForProperty(row, "id", Long.class)).thenReturn(11L);
        when(cqlRowInvoker.invokeOnRowForProperty(row, "name", String.class)).thenReturn("COMPOUND_ID");

        Object actual = mapper.createFromRow(row, pm);

        assertThat(actual).isNotNull();
        assertThat(actual).isInstanceOf(CompoundKeyByConstructorWithEnum.class);

        CompoundKeyByConstructorWithEnum compoundKey = (CompoundKeyByConstructorWithEnum) actual;

        assertThat(compoundKey.getId()).isEqualTo(11L);
        assertThat(compoundKey.getType()).isEqualTo(COMPOUND_ID);
    }

    @Test(expected = AchillesException.class)
    public void should_exception_when_component_not_found_in_row() throws Exception {
        Constructor<CompoundKey> constructor = CompoundKey.class.getConstructor();
        Method idSetter = CompoundKey.class.getDeclaredMethod("setUserId", Long.class);
        Method nameSetter = CompoundKey.class.getDeclaredMethod("setName", String.class);

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(true);
        when(pm.<CompoundKey> getCompoundKeyConstructor()).thenReturn(constructor);
        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("userid", "name"));
        when(pm.getComponentClasses()).thenReturn(Arrays.<Class<?>> asList(Long.class, String.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.asList(idSetter, nameSetter));

        when(row.isNull("userid")).thenReturn(true);

        mapper.createFromRow(row, pm);
    }

    @Test
    public void should_extract_components_from_compound_key() throws Exception {
        CompoundKeyWithEnum compoundKey = new CompoundKeyWithEnum();

        Method idGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getId");
        Method typeGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getType");

        when(pm.getComponentClasses()).thenReturn(Arrays.<Class<?>> asList(Long.class, PropertyType.class));
        when(pm.getComponentGetters()).thenReturn(Arrays.asList(idGetter, typeGetter));

        when(invoker.getValueFromField(compoundKey, idGetter)).thenReturn(11L);
        when(invoker.getValueFromField(compoundKey, typeGetter)).thenReturn(COMPOUND_ID);

        List<Object> components = mapper.extractComponents(compoundKey, pm);

        assertThat(components).containsExactly(11L, "COMPOUND_ID");
    }

    @Test
    public void should_extract_components_from_compound_key_with_enum() throws Exception {
        CompoundKey compoundKey = new CompoundKey(11L, "DuyHai");

        Method idGetter = CompoundKey.class.getDeclaredMethod("getUserId");
        Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");

        when(pm.getComponentClasses()).thenReturn(Arrays.<Class<?>> asList(Long.class, String.class));
        when(pm.getComponentGetters()).thenReturn(Arrays.asList(idGetter, nameGetter));

        when(invoker.getValueFromField(compoundKey, idGetter)).thenReturn(11L);
        when(invoker.getValueFromField(compoundKey, nameGetter)).thenReturn("DuyHai");

        List<Object> components = mapper.extractComponents(compoundKey, pm);

        assertThat(components).containsExactly(11L, "DuyHai");
    }
}
