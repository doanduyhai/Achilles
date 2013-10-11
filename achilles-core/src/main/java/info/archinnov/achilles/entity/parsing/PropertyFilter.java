/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.entity.parsing;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Id;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyFilter {
	private static final Logger log = LoggerFactory.getLogger(PropertyFilter.class);

	static final List<Class<?>> acceptedAnnotations = new ArrayList<Class<?>>();

	static {
		acceptedAnnotations.add(Id.class);
		acceptedAnnotations.add(EmbeddedId.class);
		acceptedAnnotations.add(Column.class);
	}

	public boolean matches(Field field) {
		log.trace("Does the field {} of class {} has the annotations @Id/@EmbeddedId/@Column ?", field.getName(), field
				.getDeclaringClass().getCanonicalName());

		for (Class<?> clazz : acceptedAnnotations) {
			if (hasAnnotation(field, clazz)) {
				return true;
			}
		}
		return false;
	}

	public boolean matches(Field field, Class<?> annotation) {
		log.trace("Does the field {} of class {} has the annotations {} ?", field.getName(), field.getDeclaringClass()
				.getCanonicalName(), annotation.getCanonicalName());
		if (hasAnnotation(field, annotation)) {
			return true;
		}
		return false;
	}

	public boolean matches(Field field, Class<?> annotation, String propertyName) {

		log.trace("Does the field {} of class {} has the annotations {} ?", field.getName(), field.getDeclaringClass()
				.getCanonicalName(), annotation.getCanonicalName());

		if (hasAnnotation(field, annotation) && field.getName().equals(propertyName)) {
			return true;
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public boolean hasAnnotation(Field field, Class<?> annotationClass) {
		log.trace("Does the field {} of class {} has the annotations {} ?", field.getName(), field.getDeclaringClass()
				.getCanonicalName(), annotationClass.getCanonicalName());
		return field.getAnnotation((Class<Annotation>) annotationClass) != null;
	}

}
