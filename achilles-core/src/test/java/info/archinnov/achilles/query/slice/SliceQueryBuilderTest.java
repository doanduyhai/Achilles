package info.archinnov.achilles.query.slice;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * ThriftSliceQueryBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryBuilderTest
{

    private SliceQueryBuilder<PersistenceContext, ClusteredEntity> builder;

    @Mock
    private SliceQueryExecutor<PersistenceContext> sliceQueryExecutor;

    @Mock
    private CompoundKeyValidator compoundKeyValidator;

    @Mock
    private DataTranscoder transcoder;

    private EntityMeta meta;

    private PropertyMeta idMeta;

    @Before
    public void setUp() throws Exception
    {
        meta = new EntityMeta();
        meta.setIdClass(CompoundKey.class);

        Method userIdGetter = CompoundKey.class.getDeclaredMethod("getUserId");
        Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");

        idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .compGetters(userIdGetter, nameGetter)
                .compClasses(Long.class, String.class)
                .transcoder(transcoder)
                .build();

        meta.setIdMeta(idMeta);

        builder = new SliceQueryBuilder<PersistenceContext, ClusteredEntity>(sliceQueryExecutor,
                compoundKeyValidator, ClusteredEntity.class, meta);
        Whitebox.setInternalState(builder, "meta", meta);
    }

    @Test
    public void should_set_partition_key_and_create_builder() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        SliceQueryBuilder<PersistenceContext, ClusteredEntity>.SliceShortcutQueryBuilder shortCutBuilder = builder
                .partitionKey(partitionKey);

        assertThat(shortCutBuilder).isNotNull();
    }

    @Test
    public void should_set_from_embedded_id_and_create_builder() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        String name = "name";
        CompoundKey compoundKey = new CompoundKey(partitionKey, name);

        List<Object> components = Arrays.<Object> asList(partitionKey, name);
        when(transcoder.encodeToComponents(idMeta, compoundKey)).thenReturn(components);

        SliceQueryBuilder<PersistenceContext, ClusteredEntity>.SliceFromEmbeddedIdBuilder embeddedIdBuilder = builder
                .fromEmbeddedId(compoundKey);

        assertThat(embeddedIdBuilder).isNotNull();
    }

    @Test
    public void should_set_to_embedded_id_and_create_builder() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        String name = "name";
        CompoundKey compoundKey = new CompoundKey(partitionKey, name);

        List<Object> components = Arrays.<Object> asList(partitionKey, name);
        when(transcoder.encodeToComponents(idMeta, compoundKey)).thenReturn(components);

        SliceQueryBuilder<PersistenceContext, ClusteredEntity>.SliceToEmbeddedIdBuilder embeddedIdBuilder = builder
                .toEmbeddedId(compoundKey);

        assertThat(embeddedIdBuilder).isNotNull();

    }
}
