package info.archinnov.achilles.entity.metadata.util;

import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.lang.reflect.Method;
import java.util.Map;

import com.google.common.base.Function;

/**
 * AlreadyLoadedTransformer
 * 
 * @author DuyHai DOAN
 * 
 */
public class AlreadyLoadedTransformer implements Function<Method, PropertyMeta<?, ?>>
{
	private final Map<Method, PropertyMeta<?, ?>> getterMetas;

	public AlreadyLoadedTransformer(Map<Method, PropertyMeta<?, ?>> getterMetas) {
		this.getterMetas = getterMetas;
	}

	@Override
	public PropertyMeta<?, ?> apply(Method getter)
	{
		return getterMetas.get(getter);
	}

}
