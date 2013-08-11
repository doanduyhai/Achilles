package info.archinnov.achilles.table;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.entity.metadata.EmbeddedIdProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.TweetCompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithEnum;
import info.archinnov.achilles.test.parser.entity.CorrectCompoundKey;
import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftComparatorTypeAliasFactoryTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @InjectMocks
    private ThriftComparatorTypeAliasFactory factory;

    @Mock
    private ThriftCompoundKeyMapper mapper;

    @Mock
    private PropertyMeta compoundKeyWideMeta;

    @Mock
    private PropertyMeta compoundKeyWithEnumWideMeta;

    @Mock
    private EmbeddedIdProperties embeddedIdProperties;

    @Mock
    private PropertyMeta embeddedIdMeta;

    @Mock
    private PropertyMeta idMeta;

    @Mock
    private List<Method> componentGetters;

    @Before
    public void setUp() {
        when(idMeta.isEmbeddedId()).thenReturn(false);
        when(embeddedIdMeta.isEmbeddedId()).thenReturn(true);
    }

    @Test
    public void should_determine_composite_type_alias_for_clustered_entity_creation() throws Exception {

        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        String actual = factory.determineCompatatorTypeAliasForClusteredEntity(idMeta, true);

        assertThat(actual).isEqualTo("(UTF8Type,UUIDType)");
    }

    @Test
    public void should_determine_composite_type_alias_for_clustered_entity_validation() throws Exception {

        PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        String actual = factory.determineCompatatorTypeAliasForClusteredEntity(idMeta, false);

        assertThat(actual).isEqualTo(
                "CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");
    }
}
