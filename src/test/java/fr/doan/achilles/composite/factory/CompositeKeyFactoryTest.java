package fr.doan.achilles.composite.factory;

import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static fr.doan.achilles.serializer.Utils.UUID_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.helper.CompositeHelper;

/**
 * CompositeKeyFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CompositeKeyFactoryTest {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @InjectMocks
    private CompositeKeyFactory factory;

    @Mock
    private CompositeHelper helper;

    @SuppressWarnings("unchecked")
    private List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ);
    private List<Object> keyValues = Arrays.asList((Object) 1, "sdf");

    @Before
    public void setUp() {
        ReflectionTestUtils.setField(factory, "helper", helper);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_build_for_insert() throws Exception {
        List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ, UUID_SRZ);
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        List<Object> keyValues = Arrays.asList((Object) 1, "a", uuid);

        Composite comp = factory.createForInsert("property", keyValues, serializers);

        assertThat(comp.getComponents()).hasSize(3);
        assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(1);
        assertThat((String) comp.getComponents().get(1).getValue()).isEqualTo("a");
        assertThat((UUID) comp.getComponents().get(2).getValue()).isEqualTo(uuid);
    }

    @Test
    public void should_build_for_insert_single_value() throws Exception {

        Composite comp = factory.createForInsert("property", "a", STRING_SRZ);

        assertThat(comp.getComponents()).hasSize(1);
        assertThat((String) comp.getComponents().get(0).getValue()).isEqualTo("a");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_exception_when_missing_value() throws Exception {
        List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ, UUID_SRZ);
        List<Object> keyValues = Arrays.asList((Object) 1, "a");

        expectedEx.expect(ValidationException.class);
        expectedEx.expectMessage("There should be 3 values for the key of WideMap 'property'");

        factory.createForInsert("property", keyValues, serializers);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_exception_when_null_value() throws Exception {
        List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ, UUID_SRZ);
        List<Object> keyValues = Arrays.asList((Object) 1, "a", null);

        expectedEx.expect(ValidationException.class);
        expectedEx.expectMessage("The values for the for the key of WideMap 'property' should not be null");

        factory.createForInsert("property", keyValues, serializers);

    }

    @Test
    public void should_build_multikey_for_query() throws Exception {

        when(helper.findLastNonNullIndexForComponents("property", keyValues)).thenReturn(1);

        Composite comp = factory.createForQueryMultiKey("property", keyValues, serializers, LESS_THAN_EQUAL);

        assertThat(comp.getComponents()).hasSize(2);

        assertThat(comp.getComponents().get(0).getEquality()).isEqualTo(EQUAL);
        assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
    }

    @Test
    public void should_build_multikey_for_query_start_inclusive() throws Exception {

        when(helper.findLastNonNullIndexForComponents("property", keyValues)).thenReturn(1);

        Composite comp = factory.createForQueryMultiKeyStart("property", keyValues, serializers, true);

        assertThat(comp.getComponents()).hasSize(2);
        assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(EQUAL);
    }

    @Test
    public void should_build_multikey_for_query_start_exclusive() throws Exception {

        when(helper.findLastNonNullIndexForComponents("property", keyValues)).thenReturn(1);

        Composite comp = factory.createForQueryMultiKeyStart("property", keyValues, serializers, false);

        assertThat(comp.getComponents()).hasSize(2);
        assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(GREATER_THAN_EQUAL);

    }

    @Test
    public void should_build_multikey_for_query_end_inclusive() throws Exception {
        when(helper.findLastNonNullIndexForComponents("property", keyValues)).thenReturn(1);

        Composite comp = factory.createForQueryMultiKeyEnd("property", keyValues, serializers, true);

        assertThat(comp.getComponents()).hasSize(2);
        assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
    }

    @Test
    public void should_build_multikey_for_query_end_exclusive() throws Exception {
        when(helper.findLastNonNullIndexForComponents("property", keyValues)).thenReturn(1);

        Composite comp = factory.createForQueryMultiKeyEnd("property", keyValues, serializers, false);

        assertThat(comp.getComponents()).hasSize(2);
        assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
    }

    @Test
    public void should_build_single_value_for_query() throws Exception {
        Composite comp = factory.createForQuery("a", LESS_THAN_EQUAL);

        assertThat(comp.getComponents()).hasSize(1);
        assertThat(comp.getComponents().get(0).getEquality()).isEqualTo(LESS_THAN_EQUAL);
    }

    @Test
    public void should_build_null_value_for_query() throws Exception {
        Composite comp = factory.createForQuery(null, LESS_THAN_EQUAL);
        assertThat(comp).isNull();
    }

    @Test
    public void should_build_base_for_query() throws Exception {
        Composite comp = factory.createBaseForQuery("a");
        assertThat(comp.getComponent(0).getValue()).isEqualTo("a");
        assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
    }

    @Test
    public void should_build_composites_for_query() throws Exception {

        when(helper.determineEquality(true, false, false)).thenReturn(
                new ComponentEquality[] { EQUAL, GREATER_THAN_EQUAL });

        Composite[] comps = factory.createForQuery("a", true, "b", false, false);

        assertThat(comps).hasSize(2);
        assertThat(comps[0].getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(comps[1].getComponent(0).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
    }
}
