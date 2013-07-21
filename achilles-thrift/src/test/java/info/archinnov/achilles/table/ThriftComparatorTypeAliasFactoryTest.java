package info.archinnov.achilles.table;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.entity.metadata.CompoundKeyProperties;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.TweetCompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithEnum;
import info.archinnov.achilles.test.parser.entity.CorrectCompoundKey;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.Maps;

@RunWith(MockitoJUnitRunner.class)
public class ThriftComparatorTypeAliasFactoryTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @InjectMocks
    private ThriftComparatorTypeAliasFactory factory;

    @Mock
    private ThriftCompoundKeyMapper mapper;

    @Mock
    private PropertyMeta<TweetCompoundKey, String> compoundKeyWideMeta;

    @Mock
    private PropertyMeta<CompoundKeyWithEnum, String> compoundKeyWithEnumWideMeta;

    @Mock
    private CompoundKeyProperties compoundKeyProperties;

    @Mock
    private PropertyMeta<CorrectCompoundKey, String> multiKeyWideMapMeta;

    @Mock
    private PropertyMeta<Integer, String> wideMapMeta;

    @Mock
    private List<Method> componentGetters;

    @Before
    public void setUp() {
        when(wideMapMeta.isSingleKey()).thenReturn(true);
        when(multiKeyWideMapMeta.isSingleKey()).thenReturn(false);
    }

    @Test
    public void should_determine_composite_type_alias_for_column_family_check() throws Exception {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
        propertyMeta.setType(PropertyType.WIDE_MAP);
        propertyMeta.setKeyClass(Integer.class);
        Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
        propertyMap.put("map", propertyMeta);
        entityMeta.setPropertyMetas(propertyMap);

        String compatatorTypeAlias = factory.determineCompatatorTypeAliasForCompositeCF(propertyMeta, false);

        assertThat(compatatorTypeAlias).isEqualTo("CompositeType(org.apache.cassandra.db.marshal.BytesType)");
    }

    @Test
    public void should_determine_composite_type_alias_for_column_family() throws Exception {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
        propertyMeta.setType(PropertyType.WIDE_MAP);
        propertyMeta.setKeyClass(Integer.class);
        Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
        propertyMap.put("map", propertyMeta);
        entityMeta.setPropertyMetas(propertyMap);

        String compatatorTypeAlias = factory.determineCompatatorTypeAliasForCompositeCF(propertyMeta, true);

        assertThat(compatatorTypeAlias).isEqualTo("(BytesType)");
    }

    @Test
    public void should_determine_composite_type_alias_for_multikey_column_family() throws Exception {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta<TweetCompoundKey, String> propertyMeta = new PropertyMeta<TweetCompoundKey, String>();
        propertyMeta.setType(PropertyType.WIDE_MAP);
        propertyMeta.setKeyClass(TweetCompoundKey.class);
        Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
        propertyMap.put("values", propertyMeta);
        entityMeta.setPropertyMetas(propertyMap);

        String compatatorTypeAlias = factory.determineCompatatorTypeAliasForCompositeCF(propertyMeta, true);

        assertThat(compatatorTypeAlias).isEqualTo("(UUIDType,UTF8Type,BytesType)");
    }

    @Test
    public void should_determine_composite_type_alias_for_multikey_column_family_check() throws Exception {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta<TweetCompoundKey, String> propertyMeta = new PropertyMeta<TweetCompoundKey, String>();
        propertyMeta.setType(PropertyType.WIDE_MAP);
        propertyMeta.setKeyClass(TweetCompoundKey.class);
        Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
        propertyMap.put("values", propertyMeta);
        entityMeta.setPropertyMetas(propertyMap);

        String compatatorTypeAlias = factory.determineCompatatorTypeAliasForCompositeCF(propertyMeta, false);

        assertThat(compatatorTypeAlias)
                .isEqualTo(
                        "CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.BytesType)");
    }

    @Test
    public void should_determine_composite_type_alias_for_clustered_entity_creation() throws Exception {

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        String actual = factory.determineCompatatorTypeAliasForClusteredEntity(idMeta, true);

        assertThat(actual).isEqualTo("(UTF8Type,UUIDType)");
    }

    @Test
    public void should_determine_composite_type_alias_for_clustered_entity_validation() throws Exception {

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        String actual = factory.determineCompatatorTypeAliasForClusteredEntity(idMeta, false);

        assertThat(actual).isEqualTo(
                "CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");
    }
}
