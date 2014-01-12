package info.archinnov.achilles.internal.reflection;

import java.lang.reflect.Field;

public class FieldAccessor {

	@SuppressWarnings("unchecked")
	public <T> T getValueFromField(Field field, Object instance) throws IllegalAccessException {
		T result = null;
		if (instance != null) {
			boolean isAccessible = field.isAccessible();
			if (!isAccessible) {
				field.setAccessible(true);
			}
			result = (T) field.get(instance);
			if (!isAccessible) {
				field.setAccessible(false);
			}
		}

		return result;

	}

	public void setValueToField(Field field, Object instance, Object value) throws IllegalAccessException {
		if (instance != null) {
			boolean isAccessible = field.isAccessible();
			if (!isAccessible) {
				field.setAccessible(true);
			}
			field.set(instance, value);
			if (!isAccessible) {
				field.setAccessible(false);
			}
		}
	}
}
