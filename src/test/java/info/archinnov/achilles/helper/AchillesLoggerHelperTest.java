package info.archinnov.achilles.helper;

import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import mapping.entity.CompleteBean;

import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * AchillesLoggerHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesLoggerHelperTest
{

	@Test
	public void should_transform_class_list_to_canonical_class_name_list() throws Exception
	{
		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(Long.class);

		assertThat(Lists.transform(classes, AchillesLoggerHelper.fqcnToStringFn)).contains(
				Long.class.getCanonicalName());
	}

	@Test
	public void should_transform_field_list_to_field_name_list() throws Exception
	{
		Field field = CompleteBean.class.getDeclaredField("id");
		List<Field> fields = new ArrayList<Field>();
		fields.add(field);

		assertThat(Lists.transform(fields, AchillesLoggerHelper.fieldToStringFn)).contains("id");
	}
}
