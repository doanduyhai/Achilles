package fr.doan.achilles.entity.operations;

import java.io.Serializable;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.builder.EntityProxyBuilder;

/**
 * JoinEntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinEntityLoader
{

	private EntityLoader loader = new EntityLoader();
	private EntityProxyBuilder interceptorBuilder = new EntityProxyBuilder();

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <JOIN_ID, V> V loadJoinEntity(JOIN_ID joinId, PropertyMeta<?, V> joinPropertyMeta)
	{
		EntityMeta joinEntityMeta = joinPropertyMeta.getJoinMetaHolder().getEntityMeta();
		V joinEntity = (V) this.loader.load(joinPropertyMeta.getValueClass(),
				(Serializable) joinId, joinEntityMeta);

		return (V) this.interceptorBuilder.build(joinEntity, joinEntityMeta);
	}

}
