package info.archinnov.achilles.compound;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.serializer.ThriftEnumSerializer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.UUID;
import mapping.entity.TweetCompoundKey;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
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
import parser.entity.CompoundKeyByConstructor;
import parser.entity.CompoundKeyByConstructorWithEnum;
import parser.entity.CompoundKeyWithEnum;

@RunWith(MockitoJUnitRunner.class)
public class ThriftCompoundKeyMapperTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @InjectMocks
    private ThriftCompoundKeyMapper factory;

    @Mock
    private PropertyMeta<TweetCompoundKey, String> compoundKeyMeta;

    @Mock
    private PropertyMeta<CompoundKeyWithEnum, String> compoundKeyWithEnumMeta;

    @Mock
    private PropertyMeta<CompoundKeyByConstructor, String> compoundKeyByConstructorMeta;

    @Mock
    private PropertyMeta<CompoundKeyByConstructorWithEnum, String> compoundKeyByConstructorWithEnumMeta;

    @Test
    public void should_build_compound_key() throws Exception
    {
        Method authorSetter = TweetCompoundKey.class.getDeclaredMethod("setAuthor", String.class);
        Method idSetter = TweetCompoundKey.class.getDeclaredMethod("setId", UUID.class);
        Method retweetCountSetter = TweetCompoundKey.class.getDeclaredMethod("setRetweetCount",
                Integer.class);

        Constructor<TweetCompoundKey> constructor = TweetCompoundKey.class.getConstructor();

        UUID uuid1 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        HColumn<Composite, String> hCol1 = buildHColumn(buildComposite("author1", uuid1, 11),
                "val1");

        when(compoundKeyMeta.hasDefaultConstructorForCompoundKey()).thenReturn(true);
        when(compoundKeyMeta.<TweetCompoundKey> getCompoundKeyConstructor()).thenReturn(constructor);
        when(compoundKeyMeta.getKeyClass()).thenReturn(TweetCompoundKey.class);
        when(compoundKeyMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(String.class, UUID.class, Integer.class));
        when(compoundKeyMeta.getComponentSetters()).thenReturn(
                Arrays.asList(authorSetter, idSetter, retweetCountSetter));

        TweetCompoundKey compoundKey = factory.readFromComposite(compoundKeyMeta, hCol1
                .getName()
                .getComponents());

        assertThat(compoundKey.getAuthor()).isEqualTo("author1");
        assertThat(compoundKey.getId()).isEqualTo(uuid1);
        assertThat(compoundKey.getRetweetCount()).isEqualTo(11);
    }

    @Test
    public void should_build_compound_key_with_enum() throws Exception
    {
        Method idSetter = CompoundKeyWithEnum.class.getDeclaredMethod("setId", Long.class);
        Method typeSetter = CompoundKeyWithEnum.class.getDeclaredMethod("setType", PropertyType.class);
        Constructor<CompoundKeyWithEnum> constructor = CompoundKeyWithEnum.class.getConstructor();

        Long id = RandomUtils.nextLong();

        HColumn<Composite, String> hCol1 = buildHColumn(buildCompositeWithEnum(id, COMPOUND_ID),
                "val1");

        when(compoundKeyWithEnumMeta.hasDefaultConstructorForCompoundKey()).thenReturn(true);
        when(compoundKeyWithEnumMeta.<CompoundKeyWithEnum> getCompoundKeyConstructor()).thenReturn(constructor);
        when(compoundKeyWithEnumMeta.getKeyClass()).thenReturn(CompoundKeyWithEnum.class);
        when(compoundKeyWithEnumMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, PropertyType.class));
        when(compoundKeyWithEnumMeta.getComponentSetters()).thenReturn(
                Arrays.asList(idSetter, typeSetter));

        CompoundKeyWithEnum compoundKey = factory.readFromComposite(compoundKeyWithEnumMeta, hCol1
                .getName()
                .getComponents());

        assertThat(compoundKey.getId()).isEqualTo(id);
        assertThat(compoundKey.getType()).isEqualTo(COMPOUND_ID);
    }

    @Test
    public void should_build_compound_key_from_constructor() throws Exception {

        Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class.getConstructor(
                Long.class, String.class);

        HColumn<Composite, String> hCol1 = buildHColumn(buildComposite(11L, "name"),
                "val1");

        when(compoundKeyByConstructorMeta.hasDefaultConstructorForCompoundKey()).thenReturn(false);
        when(compoundKeyByConstructorMeta.<CompoundKeyByConstructor> getCompoundKeyConstructor()).thenReturn(
                constructor);
        when(compoundKeyByConstructorMeta.getKeyClass()).thenReturn(CompoundKeyByConstructor.class);
        when(compoundKeyByConstructorMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class));

        CompoundKeyByConstructor compoundKey = factory.readFromComposite(compoundKeyByConstructorMeta,
                hCol1
                        .getName()
                        .getComponents());

        assertThat(compoundKey.getId()).isEqualTo(11L);
        assertThat(compoundKey.getName()).isEqualTo("name");
    }

    @Test
    public void should_build_compound_key_from_constructor_with_enum() throws Exception {

        Constructor<CompoundKeyByConstructorWithEnum> constructor = CompoundKeyByConstructorWithEnum.class
                .getConstructor(Long.class,
                        PropertyType.class);

        HColumn<Composite, String> hCol1 = buildHColumn(buildCompositeWithEnum(11L, COMPOUND_ID),
                "val1");

        when(compoundKeyByConstructorWithEnumMeta.hasDefaultConstructorForCompoundKey()).thenReturn(false);
        when(compoundKeyByConstructorWithEnumMeta.<CompoundKeyByConstructorWithEnum> getCompoundKeyConstructor())
                .thenReturn(constructor);
        when(compoundKeyByConstructorWithEnumMeta.getKeyClass()).thenReturn(CompoundKeyByConstructorWithEnum.class);
        when(compoundKeyByConstructorWithEnumMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, PropertyType.class));

        CompoundKeyByConstructorWithEnum compoundKey = factory.readFromComposite(
                compoundKeyByConstructorWithEnumMeta, hCol1.getName().getComponents());

        assertThat(compoundKey.getId()).isEqualTo(11L);
        assertThat(compoundKey.getType()).isEqualTo(COMPOUND_ID);
    }

    @Test
    public void should_create_composite_for_compound_key_insert() throws Exception
    {
        Long id = RandomUtils.nextLong();
        CompoundKeyWithEnum cpKey = new CompoundKeyWithEnum();
        cpKey.setId(id);
        cpKey.setType(COMPOUND_ID);

        Method idGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getId");
        Method typeGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getType");

        when(compoundKeyMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, PropertyType.class));
        when(compoundKeyMeta.getComponentGetters()).thenReturn(
                Arrays.asList(idGetter, typeGetter));

        Composite comp = factory.writeToComposite(cpKey, compoundKeyMeta);

        Serializer<Enum<PropertyType>> srz = new ThriftEnumSerializer<PropertyType>(PropertyType.class);

        assertThat(comp.getComponents()).hasSize(2);
        assertThat(comp.getComponents().get(0).getValue(LONG_SRZ)).isEqualTo(id);
        assertThat(comp.getComponents().get(1).getValue(srz)).isEqualTo(COMPOUND_ID);
    }

    @Test
    public void should_exception_when_null_value() throws Exception
    {
        CompoundKeyWithEnum cpKey = new CompoundKeyWithEnum();
        Method idGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getId");
        Method typeGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getType");

        when(compoundKeyMeta.getPropertyName()).thenReturn("compound_key");
        when(compoundKeyMeta.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, PropertyType.class));
        when(compoundKeyMeta.getComponentGetters()).thenReturn(
                Arrays.asList(idGetter, typeGetter));
        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("The values for the @CompoundKey 'compound_key' should not be null");

        factory.writeToComposite(cpKey, compoundKeyMeta);

    }

    @Test
    public void should_create_composite_for_compound_key_query() throws Exception
    {
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        Method idGetter = TweetCompoundKey.class.getDeclaredMethod("getId");
        Method authorGetter = TweetCompoundKey.class.getDeclaredMethod("getAuthor");
        Method retweetCountGetter = TweetCompoundKey.class.getDeclaredMethod("getRetweetCount");

        when(compoundKeyMeta.getComponentClasses())
                .thenReturn(Arrays.<Class<?>> asList(UUID.class, String.class, Integer.class));
        when(compoundKeyMeta.getComponentGetters()).thenReturn(
                Arrays.asList(idGetter, authorGetter, retweetCountGetter));

        TweetCompoundKey tweetMultiKey = new TweetCompoundKey(uuid, "a", null);

        Composite comp = factory.buildCompositeForQuery(tweetMultiKey, compoundKeyMeta, LESS_THAN_EQUAL);

        assertThat(comp.getComponents()).hasSize(2);

        assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(comp.getComponent(0).getValue()).isEqualTo(uuid);

        assertThat(comp.getComponent(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
        assertThat(comp.getComponent(1).getValue()).isEqualTo("a");

    }

    @Test
    public void should_exception_when_more_values_than_components() throws Exception {

        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        Method idGetter = TweetCompoundKey.class.getDeclaredMethod("getId");
        Method authorGetter = TweetCompoundKey.class.getDeclaredMethod("getAuthor");
        Method retweetCountGetter = TweetCompoundKey.class.getDeclaredMethod("getRetweetCount");

        when(compoundKeyMeta.getPropertyName()).thenReturn("compound_key");
        when(compoundKeyMeta.getComponentClasses())
                .thenReturn(
                        Arrays.<Class<?>> asList(UUID.class, String.class));
        when(compoundKeyMeta.getComponentGetters()).thenReturn(
                Arrays.asList(idGetter, authorGetter, retweetCountGetter));

        TweetCompoundKey tweetMultiKey = new TweetCompoundKey(uuid, "a", null);

        expectedEx.expect(AchillesException.class);
        expectedEx.expectMessage("There should be at most 2 values for the @CompoundKey 'compound_key'");

        factory.buildCompositeForQuery(tweetMultiKey, compoundKeyMeta, LESS_THAN_EQUAL);

    }

    private Composite buildComposite(String author, UUID uuid, int retweetCount)
    {
        Composite composite = new Composite();
        composite.setComponent(0, author, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
        composite.setComponent(1, uuid, UUID_SRZ, UUID_SRZ.getComparatorType().getTypeName());
        composite.setComponent(2, retweetCount, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());

        return composite;
    }

    private Composite buildComposite(Long id, String name)
    {
        Composite composite = new Composite();
        composite.setComponent(0, id, LONG_SRZ, LONG_SRZ.getComparatorType().getTypeName());
        composite.setComponent(1, name, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
        return composite;
    }

    private Composite buildCompositeWithEnum(Long id, PropertyType type)
    {
        ThriftEnumSerializer<PropertyType> enumSrz = new ThriftEnumSerializer<PropertyType>(PropertyType.class);

        Composite composite = new Composite();
        composite.setComponent(0, id, LONG_SRZ, LONG_SRZ.getComparatorType().getTypeName());
        composite.setComponent(1, type, enumSrz, enumSrz.getComparatorType().getTypeName());

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
