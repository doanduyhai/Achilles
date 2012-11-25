package fr.doan.achilles.metadata.builder;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import fr.doan.achilles.metadata.SetLazyPropertyMeta;
import fr.doan.achilles.metadata.SetPropertyMeta;
import fr.doan.achilles.validation.Validator;

@SuppressWarnings("rawtypes")
public class SetPropertyMetaBuilder<V extends Serializable> extends SimplePropertyMetaBuilder<V>
{

	private Class<? extends Set> setClass;

	public static <V extends Serializable> SetPropertyMetaBuilder<V> setPropertyMetaBuilder(Class<V> valueClass)
	{
		return new SetPropertyMetaBuilder<V>(valueClass);
	}

	public SetPropertyMetaBuilder(Class<V> valueClass) {
		super(valueClass);
	}

	@Override
	public SetPropertyMeta<V> build()
	{

		Validator.validateNotNull(setClass, "setClass");
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
		if (setClass == Set.class)
		{
			meta.setSetClass(HashSet.class);
		}
		else
		{
			meta.setSetClass(setClass);
		}
		return meta;
	}

	public SetPropertyMetaBuilder<V> setClass(Class<? extends Set> setClass)
	{
		this.setClass = setClass;
		return this;
	}

}
