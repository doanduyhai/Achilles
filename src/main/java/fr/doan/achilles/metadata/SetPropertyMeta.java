package fr.doan.achilles.metadata;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import fr.doan.achilles.validation.Validator;

@SuppressWarnings("rawtypes")
public class SetPropertyMeta<V extends Serializable> extends SimplePropertyMeta<V>
{

	private Class<? extends Set> setClass;

	public SetPropertyMeta(String name, Class<V> valueClazz, Class<? extends Set> setClazz) {
		super(name, valueClazz);
		Validator.validateNotNull(setClazz, "setClazz");
		Validator.validateNoargsConstructor(setClazz);
		if (setClazz == Set.class)
		{
			this.setClass = HashSet.class;
		}
		else
		{
			this.setClass = setClazz;
		}
	}

	@SuppressWarnings("unchecked")
	public Set<V> newSetInstance() throws InstantiationException, IllegalAccessException
	{
		return this.setClass.newInstance();
	}

	@Override
	public PropertyType propertyType()
	{
		return PropertyType.SET;
	}
}
