package fr.doan.achilles.entity.metadata.builder;

import fr.doan.achilles.entity.metadata.SetLazyMeta;
import fr.doan.achilles.entity.metadata.SetMeta;

public class SetMetaBuilder<V> extends SimpleMetaBuilder<V>
{

	public static <V> SetMetaBuilder<V> setMetaBuilder(Class<V> valueClass)
	{
		return new SetMetaBuilder<V>(valueClass);
	}

	public SetMetaBuilder(Class<V> valueClass) {
		super(valueClass);
	}

	@Override
	public SetMeta<V> build()
	{
		SetMeta<V> meta;
		if (this.lazy)
		{
			meta = new SetLazyMeta<V>();
		}
		else
		{
			meta = new SetMeta<V>();
		}

		super.build(meta);
		return meta;
	}
}
