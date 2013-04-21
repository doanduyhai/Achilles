package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import testBuilders.PropertyMetaTestBuilder;

/**
 * EntityMetaTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMetaTest
{
	@Test
	public void should_to_string() throws Exception
	{
		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetas.put("name", null);
		propertyMetas.put("age", null);

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.type(PropertyType.SIMPLE)//
				.consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ALL, ALL))//
				.build();

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setClassName("className");
		entityMeta.setColumnFamilyName("cfName");
		entityMeta.setSerialVersionUID(10L);
		entityMeta.setIdSerializer(LONG_SRZ);
		entityMeta.setPropertyMetas(propertyMetas);
		entityMeta.setIdMeta(idMeta);
		entityMeta.setColumnFamilyDirectMapping(true);
		entityMeta.setConsistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ONE, ONE));

		StringBuilder toString = new StringBuilder();
		toString.append("EntityMeta [className=className, ");
		toString.append("columnFamilyName=cfName, ");
		toString.append("serialVersionUID=10, ");
		toString.append("idSerializer=").append(LONG_SRZ.getComparatorType().getTypeName())
				.append(", ");
		toString.append("propertyMetas=[age,name], ");
		toString.append("idMeta=").append(idMeta.toString()).append(", ");
		toString.append("columnFamilyDirectMapping=true, ");
		toString.append("consistencyLevels=[ONE,ONE]]");
		assertThat(entityMeta.toString()).isEqualTo(toString.toString());
	}
}
