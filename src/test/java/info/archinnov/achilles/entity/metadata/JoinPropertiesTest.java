package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import javax.persistence.CascadeType;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

/**
 * JoinPropertiesTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinPropertiesTest
{
	@Test
	public void should_to_string() throws Exception
	{
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setClassName("className");

		JoinProperties props = new JoinProperties();
		props.setEntityMeta(entityMeta);
		props.addCascadeType(CascadeType.MERGE);
		props.addCascadeType(CascadeType.PERSIST);

		StringBuilder toString = new StringBuilder();
		toString.append("JoinProperties [entityMeta=className, ");
		toString.append("cascadeTypes=[").append(StringUtils.join(props.getCascadeTypes(), ","))
				.append("]]");

		assertThat(props.toString()).isEqualTo(toString.toString());
	}
}
