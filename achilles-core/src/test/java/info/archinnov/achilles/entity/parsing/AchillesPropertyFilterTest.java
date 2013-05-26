package info.archinnov.achilles.entity.parsing;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.annotations.Lazy;

import java.lang.reflect.Field;

import mapping.entity.CompleteBean;

import org.junit.Test;

import parser.entity.BeanWithClusteredId;
import parser.entity.ParentBean;

/**
 * AchillesPropertyFilterTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesPropertyFilterTest
{
	private AchillesPropertyFilter filter = new AchillesPropertyFilter();

	@Test
	public void should_match() throws Exception
	{
		Field name = CompleteBean.class.getDeclaredField("name");

		assertThat(filter.matches(name)).isTrue();
	}

	@Test
	public void should_match_embedded_id() throws Exception
	{
		Field clusteredId = BeanWithClusteredId.class.getDeclaredField("id");

		assertThat(filter.matches(clusteredId)).isTrue();
	}

	@Test
	public void should_match_annotation() throws Exception
	{
		Field friends = CompleteBean.class.getDeclaredField("friends");

		assertThat(filter.matches(friends, Lazy.class)).isTrue();
	}

	@Test
	public void should_match_annotation_and_name() throws Exception
	{
		Field friends = CompleteBean.class.getDeclaredField("friends");

		assertThat(filter.matches(friends, Lazy.class, "friends")).isTrue();
	}

	@Test
	public void should_not_match() throws Exception
	{
		Field name = ParentBean.class.getDeclaredField("unmapped");

		assertThat(filter.matches(name)).isFalse();
	}

	@Test
	public void should_have_annotation() throws Exception
	{
		Field name = CompleteBean.class.getDeclaredField("name");

		assertThat(filter.hasAnnotation(name, javax.persistence.Column.class)).isTrue();
	}

	@Test
	public void should_not_have_annotation() throws Exception
	{
		Field name = CompleteBean.class.getDeclaredField("name");

		assertThat(filter.hasAnnotation(name, javax.persistence.JoinColumn.class)).isFalse();
	}
}
