package info.archinnov.achilles.entity.parser;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger log = LoggerFactory.getLogger(PropertyFilter.class);

	public static List<Class> acceptedAnnotations = Arrays.asList(
			(Class) javax.persistence.Id.class, javax.persistence.Column.class,
			javax.persistence.JoinColumn.class);

	public boolean matches(Field field)
	{
		if (log.isTraceEnabled())
		{
			log.trace(
					"Does the field {} of class {} has the annotations @Id/@Column/@JoinColumn ?",
					field.getName(), field.getDeclaringClass().getCanonicalName());
		}

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
		if (log.isTraceEnabled())
		{
			log.trace("Does the field {} of class {} has the annotations {} ?", field.getName(),
					field.getDeclaringClass().getCanonicalName(), annotation.getCanonicalName());
		}
		if (field.getAnnotation(annotation) != null)
		{
			return true;
		}
		return false;
	}

	public boolean matches(Field field, Class annotation, String propertyName)
	{

		if (log.isTraceEnabled())
		{
			log.trace("Does the field {} of class {} has the annotations {} ?", field.getName(),
					field.getDeclaringClass().getCanonicalName(), annotation.getCanonicalName());
		}

		if (field.getAnnotation(annotation) != null && field.getName().equals(propertyName))
		{
			return true;
		}
		return false;
	}

	public <T extends Annotation> boolean hasAnnotation(Field field, Class<T> annotationClass)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Does the field {} of class {} has the annotations {} ?", field.getName(),
					field.getDeclaringClass().getCanonicalName(),
					annotationClass.getCanonicalName());
		}
		return field.getAnnotation(annotationClass) != null;
	}
}
