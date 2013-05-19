package info.archinnov.achilles.helper;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.format;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.Composite;

import org.junit.Test;

/**
 * LoggerHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class LoggerHelperTest
{

	@Test
	public void should_format_null_composite() throws Exception
	{
		assertThat(format((Composite) null)).isEqualTo("null");
	}

	@Test
	public void should_format_multi_components() throws Exception
	{
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		Composite comp = new Composite();
		comp.add(0, "text");
		comp.add(1, 12L);
		comp.add(2, uuid);

		assertThat(format(comp)).isEqualTo("[text:12:" + uuid + "(EQUAL)]");

	}

	@Test
	public void should_format_single_component() throws Exception
	{
		Composite comp = new Composite();
		comp.add(0, "text");
		assertThat(format(comp)).isEqualTo("[text(EQUAL)]");
	}

	@Test
	public void should_format_empty_composite() throws Exception
	{
		Composite comp = new Composite();
		assertThat(format(comp)).isEqualTo("[]");
	}

	@Test
	public void should_format_with_component_equality() throws Exception
	{
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		Composite comp = new Composite();
		comp.addComponent(0, "text", LESS_THAN_EQUAL);
		comp.addComponent(1, 12L, GREATER_THAN_EQUAL);
		comp.addComponent(2, uuid, GREATER_THAN_EQUAL);

		assertThat(format(comp)).isEqualTo("[text:12:" + uuid + "(GREATER_THAN_EQUAL)]");
	}

	@Test
	public void should_format_with_byte_array() throws Exception
	{
		Composite comp = new Composite();
		comp.addComponent(0, PropertyType.COUNTER.flag(), EQUAL);
		comp.addComponent(1, "test", GREATER_THAN_EQUAL);

		assertThat(format(comp)).isEqualTo("[70:test(GREATER_THAN_EQUAL)]");
	}
}
