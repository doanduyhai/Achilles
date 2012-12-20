package fr.doan.achilles.wrapper.factory;

import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static fr.doan.achilles.serializer.Utils.UUID_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import fr.doan.achilles.exception.ValidationException;

/**
 * CompositeKeyFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompositeKeyFactoryTest
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();
	private CompositeKeyFactory factory = new CompositeKeyFactory();

	@SuppressWarnings("unchecked")
	private List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ);
	private List<Object> keyValues = Arrays.asList((Object) 1, "sdf");

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_for_insert() throws Exception
	{
		List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ,
				UUID_SRZ);
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		List<Object> keyValues = Arrays.asList((Object) 1, "a", uuid);

		Composite comp = factory.buildForInsert("property", keyValues, serializers);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(1);
		assertThat((String) comp.getComponents().get(1).getValue()).isEqualTo("a");
		assertThat((UUID) comp.getComponents().get(2).getValue()).isEqualTo(uuid);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_exception_when_missing_value() throws Exception
	{
		List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ,
				UUID_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1, "a");

		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("There should be 3 values for the key of WideMap 'property'");

		factory.buildForInsert("property", keyValues, serializers);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_exception_when_null_value() throws Exception
	{
		List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ,
				UUID_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1, "a", null);

		expectedEx.expect(ValidationException.class);
		expectedEx
				.expectMessage("The values for the for the key of WideMap 'property' should not be null");

		factory.buildForInsert("property", keyValues, serializers);

	}

	@Test
	public void should_build_multikey_query_comparator() throws Exception
	{
		Composite comp = factory.buildQueryComparator("property", keyValues, serializers,
				LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(2);

		assertThat(comp.getComponents().get(0).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
	}

	@Test
	public void should_build_multikey_query_comparator_start_inclusive() throws Exception
	{
		Composite comp = factory
				.buildQueryComparatorStart("property", keyValues, serializers, true);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(EQUAL);
	}

	@Test
	public void should_build_multikey_query_comparator_start_exclusive() throws Exception
	{
		Composite comp = factory.buildQueryComparatorStart("property", keyValues, serializers,
				false);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(GREATER_THAN_EQUAL);

	}

	@Test
	public void should_build_multikey_query_comparator_end_inclusive() throws Exception
	{
		Composite comp = factory.buildQueryComparatorEnd("property", keyValues, serializers, true);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_build_multikey_query_comparator_end_exclusive() throws Exception
	{
		Composite comp = factory.buildQueryComparatorEnd("property", keyValues, serializers, false);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponents().get(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);

	}
}
