package fr.doan.achilles.entity.metadata.builder;

import fr.doan.achilles.entity.metadata.SetLazyPropertyMeta;
import fr.doan.achilles.entity.metadata.SetPropertyMeta;

public class SetPropertyMetaBuilder<V> extends SimplePropertyMetaBuilder<V>
{

	public static <V> SetPropertyMetaBuilder<V> setPropertyMetaBuilder(Class<V> valueClass)
	{
		return new SetPropertyMetaBuilder<V>(valueClass);
	}

	public SetPropertyMetaBuilder(Class<V> valueClass) {
		super(valueClass);
	}

	@Override
	public SetPropertyMeta<V> build()
	{
		SetPropertyMeta<V> meta;
		if (this.lazy)
		{
			meta = new SetLazyPropertyMeta<V>();
		}
		else
		{
			meta = new SetPropertyMeta<V>();
		}

		super.build(meta);
		return meta;
	}
}
