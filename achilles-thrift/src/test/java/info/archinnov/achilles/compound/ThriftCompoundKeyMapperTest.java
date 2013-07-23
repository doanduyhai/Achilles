package info.archinnov.achilles.compound;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.TweetCompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKeyByConstructor;
import info.archinnov.achilles.test.parser.entity.CompoundKeyByConstructorWithEnum;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithEnum;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftCompoundKeyMapperTest
{

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @InjectMocks
    private ThriftCompoundKeyMapper mapper;

    @Mock
    private ThriftCompoundKeyValidator validator;

    @Mock
    private PropertyMeta<TweetCompoundKey, String> compoundKeyMeta;

    @Mock
    private PropertyMeta<CompoundKeyWithEnum, String> compoundKeyWithEnumMeta;

    @Mock
    private PropertyMeta<CompoundKeyByConstructor, String> compoundKeyByConstructorMeta;

    @Mock
    private PropertyMeta<CompoundKeyByConstructorWithEnum, String> compoundKeyByConstructorWithEnumMeta;

    @Mock
    private DataTranscoder transcoder;

    @Test
    public void should_build_compound_key() throws Exception
    {
        TweetCompoundKey compoundKey = new TweetCompoundKey();
        UUID uuid1 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        HColumn<Composite, String> hCol1 = buildHColumn(buildComposite("author1", uuid1, 11),
                "val1");

        List<Class<?>> componentValues = Arrays.<Class<?>> asList(String.class, UUID.class, Integer.class);
        when(compoundKeyMeta.getComponentClasses()).thenReturn(componentValues);
        when(compoundKeyMeta.decodeFromComponents(any(List.class))).thenReturn(compoundKey);

        TweetCompoundKey actual = mapper.fromCompositeToCompound(compoundKeyMeta, hCol1
                .getName().getComponents());

        assertThat(actual).isSameAs(compoundKey);
    }

    @Test
    public void should_build_embedded_id() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        String name = "name";
        CompoundKey compoundKey = new CompoundKey();
        HColumn<Composite, String> hCol1 = buildHColumn(buildComposite(name), "val1");

        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .compClasses(Arrays.<Class<?>> asList(Long.class, String.class))
                .transcoder(transcoder)
                .build();

        when(transcoder.decodeFromComponents(eq(idMeta), any(List.class))).thenReturn(compoundKey);
        CompoundKey actual = mapper.fromCompositeToEmbeddedId(idMeta, hCol1
                .getName().getComponents(), userId);

        assertThat(actual).isSameAs(compoundKey);
    }

    @Test
    public void should_create_composite_for_compound_key_insert() throws Exception
    {
        Long id = RandomUtils.nextLong();
        CompoundKeyWithEnum compoundKey = new CompoundKeyWithEnum();

        when(compoundKeyMeta.encodeToComponents(compoundKey)).thenReturn(Arrays.<Object> asList(id, "EMBEDDED_ID"));
        when(compoundKeyMeta.isEmbeddedId()).thenReturn(false);
        when(compoundKeyMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, PropertyType.class));

        Composite comp = mapper.fromCompoundToCompositeForInsertOrGet(compoundKey, compoundKeyMeta);

        assertThat(comp.getComponents()).hasSize(2);
        assertThat(comp.getComponents().get(0).getValue(LONG_SRZ)).isEqualTo(id);
        assertThat(comp.getComponents().get(1).getValue(STRING_SRZ)).isEqualTo("EMBEDDED_ID");
    }

    @Test
    public void should_create_composite_for_embedded_id_insert() throws Exception
    {
        Long id = RandomUtils.nextLong();
        CompoundKeyWithEnum compoundKey = new CompoundKeyWithEnum();

        when(compoundKeyMeta.encodeToComponents(compoundKey)).thenReturn(Arrays.<Object> asList(id, "EMBEDDED_ID"));
        when(compoundKeyMeta.isEmbeddedId()).thenReturn(true);
        when(compoundKeyMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, PropertyType.class));

        Composite comp = mapper.fromCompoundToCompositeForInsertOrGet(compoundKey, compoundKeyMeta);

        assertThat(comp.getComponents()).hasSize(1);
        assertThat(comp.getComponents().get(0).getValue(STRING_SRZ)).isEqualTo("EMBEDDED_ID");
    }

    @Test
    public void should_exception_when_null_value() throws Exception
    {
        CompoundKeyWithEnum compoundKey = new CompoundKeyWithEnum();

        when(compoundKeyMeta.getPropertyName()).thenReturn("compound_key");
        List<Object> list = new ArrayList<Object>();
        list.add(null);
        when(compoundKeyMeta.encodeToComponents(compoundKey)).thenReturn(list);

        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("The component values for the @CompoundKey 'compound_key' should not be null");

        mapper.fromCompoundToCompositeForInsertOrGet(compoundKey, compoundKeyMeta);

    }

    @Test
    public void should_create_composite_for_compound_key_query() throws Exception
    {
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        when(compoundKeyMeta.isEmbeddedId()).thenReturn(false);
        when(compoundKeyMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(UUID.class, String.class, Integer.class));

        TweetCompoundKey tweetMultiKey = new TweetCompoundKey(uuid, "a", 15);

        when(compoundKeyMeta.encodeToComponents(tweetMultiKey)).thenReturn(Arrays.<Object> asList(uuid, "a", 15));
        when(validator.validateNoHoleAndReturnLastNonNullIndex(any(List.class))).thenReturn(2);
        Composite comp = mapper.fromCompoundToCompositeForQuery(tweetMultiKey, compoundKeyMeta,
                LESS_THAN_EQUAL);

        assertThat(comp.getComponents()).hasSize(3);

        assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(comp.getComponent(0).getValue()).isEqualTo(uuid);

        assertThat(comp.getComponent(1).getEquality()).isEqualTo(EQUAL);
        assertThat(comp.getComponent(1).getValue()).isEqualTo("a");

        assertThat(comp.getComponent(2).getEquality()).isEqualTo(LESS_THAN_EQUAL);
        assertThat(comp.getComponent(2).getValue()).isEqualTo(15);
    }

    @Test
    public void should_create_composite_for_embedded_id_query() throws Exception
    {
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        when(compoundKeyMeta.isEmbeddedId()).thenReturn(true);
        when(compoundKeyMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(UUID.class, String.class, Integer.class));

        TweetCompoundKey tweetMultiKey = new TweetCompoundKey(uuid, "a", 15);

        when(compoundKeyMeta.encodeToComponents(tweetMultiKey)).thenReturn(Arrays.<Object> asList(uuid, "a", 15));
        when(validator.validateNoHoleAndReturnLastNonNullIndex(any(List.class))).thenReturn(1);
        Composite comp = mapper.fromCompoundToCompositeForQuery(tweetMultiKey, compoundKeyMeta,
                LESS_THAN_EQUAL);

        assertThat(comp.getComponents()).hasSize(2);

        assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(comp.getComponent(0).getValue()).isEqualTo("a");

        assertThat(comp.getComponent(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
        assertThat(comp.getComponent(1).getValue()).isEqualTo(15);
    }

    @Test
    public void should_exception_when_more_values_than_components() throws Exception
    {
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        when(compoundKeyMeta.isEmbeddedId()).thenReturn(false);
        when(compoundKeyMeta.getPropertyName()).thenReturn("compound_key");
        when(compoundKeyMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(UUID.class, String.class));

        TweetCompoundKey tweetMultiKey = new TweetCompoundKey(uuid, "a", null);
        when(compoundKeyMeta.encodeToComponents(tweetMultiKey)).thenReturn(Arrays.<Object> asList(uuid, "a", null));

        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("There should be at most 2 values for the @CompoundKey 'compound_key'");

        mapper.fromCompoundToCompositeForQuery(tweetMultiKey, compoundKeyMeta, LESS_THAN_EQUAL);

    }

    private Composite buildComposite(String author, UUID uuid, int retweetCount)
    {
        Composite composite = new Composite();
        composite.setComponent(0, author, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
        composite.setComponent(1, uuid, UUID_SRZ, UUID_SRZ.getComparatorType().getTypeName());
        composite.setComponent(2, retweetCount, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());

        return composite;
    }

    private Composite buildComposite(String name)
    {
        Composite composite = new Composite();
        composite.setComponent(0, name, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
        return composite;
    }

    private HColumn<Composite, String> buildHColumn(Composite comp, String value)
    {
        HColumn<Composite, String> hColumn = new HColumnImpl<Composite, String>(COMPOSITE_SRZ,
                STRING_SRZ);

        hColumn.setName(comp);
        hColumn.setValue(value);
        return hColumn;
    }

}
