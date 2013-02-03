package info.archinnov.achilles.entity.parser;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

/**
 * PropertyFilter
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings(
{
		"rawtypes",
		"unchecked"
})
public class PropertyFilter
{

	public static List<Class> acceptedAnnotations = Arrays.asList(
			(Class) javax.persistence.Id.class, javax.persistence.Column.class,
			javax.persistence.JoinColumn.class);

	public boolean matches(Field field)
	{
		for (Class clazz : acceptedAnnotations)
		{
			if (field.getAnnotation(clazz) != null)
			{
				return true;
			}
		}
		return false;
	}

	public boolean matches(Field field, Class annotation)
	{
		if (field.getAnnotation(annotation) != null)
		{
			return true;
		}
		return false;
	}

	public boolean matches(Field field, Class annotation, String propertyName)
	{
		if (field.getAnnotation(annotation) != null && field.getName().equals(propertyName))
		{
			return true;
		}
		return false;
	}

	public <T extends Annotation> boolean hasAnnotation(Field field, Class<T> annotationClass)
	{
		return field.getAnnotation(annotationClass) != null;
	}
}
