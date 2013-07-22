package info.archinnov.achilles.entity;

import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Row;

/**
 * CQLEntityMapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityMapperTest
{

    @InjectMocks
    private CQLEntityMapper entityMapper;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private CQLRowMethodInvoker cqlRowInvoker;

    @Mock
    private Row row;

    @Mock
    private EntityMeta entityMeta;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Test
    public void should_set_eager_properties_to_entity() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.ID)
                .build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();

        List<PropertyMeta<?, ?>> eagerMetas = Arrays.<PropertyMeta<?, ?>> asList(pm);

        when((PropertyMeta) entityMeta.getIdMeta()).thenReturn(idMeta);
        when(entityMeta.getEagerMetas()).thenReturn(eagerMetas);

        when(row.isNull("name")).thenReturn(false);
        when(cqlRowInvoker.invokeOnRowForFields(row, pm)).thenReturn("value");

        entityMapper.setEagerPropertiesToEntity(row, entityMeta, entity);

        verify(invoker).setValueToField(entity, pm.getSetter(), "value");
    }

    @Test
    public void should_set_null_to_entity_when_no_value_from_row() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();

        List<PropertyMeta<?, ?>> eagerMetas = Arrays.<PropertyMeta<?, ?>> asList(pm);

        when(entityMeta.getEagerMetas()).thenReturn(eagerMetas);

        when(row.isNull("name")).thenReturn(true);

        entityMapper.setEagerPropertiesToEntity(row, entityMeta, entity);

        verifyZeroInteractions(cqlRowInvoker, invoker);
    }

    @Test
    public void should_do_nothing_when_null_row() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();

        entityMapper.setPropertyToEntity((Row) null, pm, entity);

        verifyZeroInteractions(cqlRowInvoker, invoker);
    }

    @Test
    public void should_set_property_to_entity() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();

        entityMapper.setJoinValueToEntity("name", pm, entity);

        verify(invoker).setValueToField(entity, pm.getSetter(), "name");
    }

    @Test
    public void should_set_compound_key_to_entity() throws Exception
    {
        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .type(PropertyType.EMBEDDED_ID)
                .compNames("name")
                .build();

        CompoundKey compoundKey = new CompoundKey();
        when(cqlRowInvoker.invokeOnRowForCompoundKey(row, pm)).thenReturn(compoundKey);

        entityMapper.setPropertyToEntity(row, pm, entity);

        verify(invoker).setValueToField(entity, pm.getSetter(), compoundKey);
    }
}
