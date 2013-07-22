package info.archinnov.achilles.compound;

import static info.archinnov.achilles.entity.metadata.PropertyType.EMBEDDED_ID;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKeyByConstructor;
import info.archinnov.achilles.test.parser.entity.CompoundKeyByConstructorWithEnum;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithEnum;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Row;

@RunWith(MockitoJUnitRunner.class)
public class CQLCompoundKeyMapperTest
{

    @InjectMocks
    private CQLCompoundKeyMapper mapper;

    @Mock
    private CQLRowMethodInvoker cqlRowInvoker;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private PropertyMeta<?, ?> pm;

    @Mock
    private Row row;

    @Test
    public void should_write_component_to_compound_key_by_setter() throws Exception
    {
        Constructor<CompoundKey> constructor = CompoundKey.class.getConstructor();
        Method idSetter = CompoundKey.class.getDeclaredMethod("setUserId", Long.class);
        Method nameSetter = CompoundKey.class.getDeclaredMethod("setName", String.class);
        CompoundKey compoundKey = new CompoundKey();

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(true);
        when(pm.<CompoundKey> getCompoundKeyConstructor()).thenReturn(constructor);
        when(invoker.instanciate(constructor)).thenReturn(compoundKey);
        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("userid", "name"));
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.asList(idSetter, nameSetter));

        when(row.isNull("userid")).thenReturn(false);
        when(row.isNull("name")).thenReturn(false);

        when(cqlRowInvoker.invokeOnRowForClusteredComponent(row, pm, "userid", Long.class)).thenReturn(11L);
        when(cqlRowInvoker.invokeOnRowForClusteredComponent(row, pm, "name", String.class)).thenReturn("DuyHai");

        Object actual = mapper.createFromRow(row, pm);

        assertThat(actual).isSameAs(compoundKey);

        verify(invoker).setValueToField(actual, idSetter, 11L);
        verify(invoker).setValueToField(actual, nameSetter, "DuyHai");
    }

    @Test
    public void should_write_component_to_compound_with_enum_key_by_setter() throws Exception
    {
        CompoundKeyWithEnum compoundKey = new CompoundKeyWithEnum();

        Constructor<CompoundKeyWithEnum> constructor = CompoundKeyWithEnum.class.getConstructor();
        Method idSetter = CompoundKeyWithEnum.class.getDeclaredMethod("setId", Long.class);
        Method typeSetter = CompoundKeyWithEnum.class.getDeclaredMethod("setType",
                PropertyType.class);

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(true);
        when(pm.<CompoundKeyWithEnum> getCompoundKeyConstructor()).thenReturn(constructor);
        when(invoker.instanciate(constructor)).thenReturn(compoundKey);

        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("id", "type"));
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, PropertyType.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.asList(idSetter, typeSetter));

        when(row.isNull("id")).thenReturn(false);
        when(row.isNull("type")).thenReturn(false);

        when(cqlRowInvoker.invokeOnRowForClusteredComponent(row, pm, "id", Long.class)).thenReturn(11L);
        when(cqlRowInvoker.invokeOnRowForClusteredComponent(row, pm, "type", PropertyType.class)).thenReturn(
                EMBEDDED_ID);

        Object actual = mapper.createFromRow(row, pm);

        assertThat(actual).isSameAs(compoundKey);

        verify(invoker).setValueToField(actual, idSetter, 11L);
        verify(invoker).setValueToField(actual, typeSetter, EMBEDDED_ID);
    }

    @Test
    public void should_write_component_to_compound_key_by_constructor() throws Exception
    {
        Long id = RandomUtils.nextLong();
        String name = "DuyHai";
        CompoundKeyByConstructor compoundKey = new CompoundKeyByConstructor(id, name);
        Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class
                .getConstructor(Long.class,
                        String.class);

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(false);
        when(pm.<CompoundKeyByConstructor> getCompoundKeyConstructor()).thenReturn(constructor);

        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("primaryKey", "name"));
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.<Method> asList());

        when(row.isNull("primaryKey")).thenReturn(false);
        when(row.isNull("name")).thenReturn(false);

        when(cqlRowInvoker.invokeOnRowForProperty(row, pm, "primaryKey", Long.class)).thenReturn(id);
        when(cqlRowInvoker.invokeOnRowForProperty(row, pm, "name", String.class)).thenReturn(name);

        when(invoker.instanciate(eq(constructor), anyVararg())).thenReturn(compoundKey);

        Object actual = mapper.createFromRow(row, pm);

        assertThat(actual).isSameAs(compoundKey);
    }

    @Test
    public void should_write_component_to_compound_key_with_enum_by_constructor() throws Exception
    {
        Long id = RandomUtils.nextLong();
        PropertyType type = EMBEDDED_ID;
        CompoundKeyByConstructorWithEnum compoundKey = new CompoundKeyByConstructorWithEnum(id,
                type);

        Constructor<CompoundKeyByConstructorWithEnum> constructor = CompoundKeyByConstructorWithEnum.class
                .getConstructor(Long.class,
                        PropertyType.class);

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(false);
        when(pm.<CompoundKeyByConstructorWithEnum> getCompoundKeyConstructor()).thenReturn(
                constructor);
        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("id", "name"));
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, PropertyType.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.<Method> asList());

        when(row.isNull("id")).thenReturn(false);
        when(row.isNull("name")).thenReturn(false);

        when(cqlRowInvoker.invokeOnRowForProperty(row, pm, "id", Long.class)).thenReturn(11L);
        when(cqlRowInvoker.invokeOnRowForProperty(row, pm, "name", String.class)).thenReturn(
                "EMBEDDED_ID");

        when(invoker.instanciate(eq(constructor), anyVararg())).thenReturn(compoundKey);

        Object actual = mapper.createFromRow(row, pm);

        assertThat(actual).isSameAs(compoundKey);

    }

    @Test(expected = AchillesException.class)
    public void should_exception_when_component_not_found_in_row() throws Exception
    {
        Constructor<CompoundKey> constructor = CompoundKey.class.getConstructor();
        Method idSetter = CompoundKey.class.getDeclaredMethod("setUserId", Long.class);
        Method nameSetter = CompoundKey.class.getDeclaredMethod("setName", String.class);

        when(pm.hasDefaultConstructorForCompoundKey()).thenReturn(true);
        when(pm.<CompoundKey> getCompoundKeyConstructor()).thenReturn(constructor);
        when(pm.getCQLComponentNames()).thenReturn(Arrays.asList("userid", "name"));
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class));
        when(pm.getComponentSetters()).thenReturn(Arrays.asList(idSetter, nameSetter));

        when(row.isNull("userid")).thenReturn(true);

        mapper.createFromRow(row, pm);
    }

    @Test
    public void should_extract_components_from_compound_key() throws Exception
    {
        CompoundKey compoundKey = new CompoundKey(11L, "DuyHai");

        Method idGetter = CompoundKey.class.getDeclaredMethod("getUserId");
        Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");

        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class));
        when(pm.getComponentGetters()).thenReturn(Arrays.asList(idGetter, nameGetter));

        when(invoker.getValueFromField(compoundKey, idGetter)).thenReturn(11L);
        when(pm.writeValueToCassandra(Long.class, 11L)).thenReturn(11L);
        when(invoker.getValueFromField(compoundKey, nameGetter)).thenReturn("DuyHai");
        when(pm.writeValueToCassandra(String.class, "DuyHai")).thenReturn("DuyHai");

        List<Object> components = mapper.extractComponents(compoundKey, pm);

        assertThat(components).containsExactly(11L, "DuyHai");
    }

    @Test
    public void should_extract_components_from_compound_key_with_enum() throws Exception
    {
        CompoundKeyWithEnum compoundKey = new CompoundKeyWithEnum();

        Method idGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getId");
        Method typeGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getType");

        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, PropertyType.class));
        when(pm.getComponentGetters()).thenReturn(Arrays.asList(idGetter, typeGetter));

        when(invoker.getValueFromField(compoundKey, idGetter)).thenReturn(11L);
        when(pm.writeValueToCassandra(Long.class, 11L)).thenReturn(11L);
        when(invoker.getValueFromField(compoundKey, typeGetter)).thenReturn(EMBEDDED_ID);
        when(pm.writeValueToCassandra(PropertyType.class, EMBEDDED_ID)).thenReturn("EMBEDDED_ID");

        List<Object> components = mapper.extractComponents(compoundKey, pm);

        assertThat(components).containsExactly(11L, "EMBEDDED_ID");
    }
}
