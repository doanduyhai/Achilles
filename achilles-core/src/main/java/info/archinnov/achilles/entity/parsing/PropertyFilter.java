package info.archinnov.achilles.entity.parsing;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyFilter
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyFilter
{
	private static final Logger log = LoggerFactory.getLogger(PropertyFilter.class);

	static final List<Class<?>> acceptedAnnotations = new ArrayList<Class<?>>();

	static
	{
		acceptedAnnotations.add(javax.persistence.Id.class);
		acceptedAnnotations.add(javax.persistence.EmbeddedId.class);
		acceptedAnnotations.add(javax.persistence.Column.class);
		acceptedAnnotations.add(javax.persistence.JoinColumn.class);
	}

	public boolean matches(Field field)
	{
		log.trace("Does the field {} of class {} has the annotations @Id/@Column/@JoinColumn ?",
				field.getName(),
				field.getDeclaringClass().getCanonicalName());

		for (Class<?> clazz : acceptedAnnotations)
		{
			if (hasAnnotation(field, clazz))
			{
				return true;
			}
		}
		return false;
	}

	public boolean matches(Field field, Class<?> annotation)
	{
		log.trace("Does the field {} of class {} has the annotations {} ?", field.getName(), field
				.getDeclaringClass()
				.getCanonicalName(), annotation.getCanonicalName());
		if (hasAnnotation(field, annotation))
		{
			return true;
		}
		return false;
	}

	public boolean matches(Field field, Class<?> annotation, String propertyName)
	{

		log.trace("Does the field {} of class {} has the annotations {} ?", field.getName(), field
				.getDeclaringClass()
				.getCanonicalName(), annotation.getCanonicalName());

		if (hasAnnotation(field, annotation) && field.getName().equals(propertyName))
		{
			return true;
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public boolean hasAnnotation(Field field, Class<?> annotationClass)
	{
		log.trace("Does the field {} of class {} has the annotations {} ?", field.getName(), field
				.getDeclaringClass()
				.getCanonicalName(), annotationClass.getCanonicalName());
		return field.getAnnotation((Class<Annotation>) annotationClass) != null;
	}

}
