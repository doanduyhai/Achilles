package fr.doan.achilles.metadata;

import static fr.doan.achilles.metadata.PropertyType.SET;
import static org.fest.assertions.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class SetPropertyMetaTest
{
	@Test
	public void should_create_new_HashSet_instance() throws Exception
	{
		SetPropertyMeta<String> setPropertyMeta = new SetPropertyMeta<String>("name", String.class, HashSet.class);

		assertThat(setPropertyMeta.newSetInstance()).isNotNull();
		assertThat(setPropertyMeta.newSetInstance()).isEmpty();
		assertThat(setPropertyMeta.newSetInstance() instanceof HashSet).isTrue();
		assertThat(setPropertyMeta.propertyType()).isEqualTo(SET);
	}

	@Test
	public void should_create_new_default_set_instance() throws Exception
	{
		SetPropertyMeta<String> setPropertyMeta = new SetPropertyMeta<String>("name", String.class, Set.class);

		assertThat(setPropertyMeta.newSetInstance()).isNotNull();
		assertThat(setPropertyMeta.newSetInstance()).isEmpty();
		assertThat(setPropertyMeta.newSetInstance() instanceof HashSet).isTrue();
		assertThat(setPropertyMeta.propertyType()).isEqualTo(SET);
	}
}
