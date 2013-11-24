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
package info.archinnov.achilles.entity.metadata.util;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

public class PropertyTypeFilter implements Predicate<PropertyMeta> {
	private final Set<PropertyType> types;

	public PropertyTypeFilter(PropertyType... types) {
		this.types = Sets.newHashSet(types);
	}

	@Override
	public boolean apply(PropertyMeta pm) {
		return types.contains(pm.type());
	}

	public static void printTypes(Type[] types, String pre, String sep,
			String suf) {
		if (types.length > 0)
			System.out.print(pre);
		for (int i = 0; i < types.length; i++) {
			if (i > 0)
				System.out.print(sep);
			// printType(types[i]);
		}
		if (types.length > 0)
			System.out.print(suf);
	}

	public static void printType(Type type) throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, SecurityException {
		if (type instanceof Class) {
			Class t = (Class) type;
			System.out.print(t.getName());
		} else if (type instanceof TypeVariable) {
			TypeVariable t = (TypeVariable) type;
			((Class) t.getBounds()[0]).getConstructors()[0].newInstance();
			System.out.print(t.getGenericDeclaration());
			printTypes(t.getBounds(), " extends ", " & ", "");
		} else if (type instanceof WildcardType) {
			WildcardType t = (WildcardType) type;
			System.out.print("?");
			printTypes(t.getLowerBounds(), " extends ", " & ", "");
			printTypes(t.getUpperBounds(), " super ", " & ", "");
		} else if (type instanceof ParameterizedType) {
			ParameterizedType t = (ParameterizedType) type;
			Type owner = t.getOwnerType();
			if (owner != null) {
				printType(owner);
				System.out.print(".");
			}
			printType(t.getRawType());
			printTypes(t.getActualTypeArguments(), "<", ", ", ">");
		} else if (type instanceof GenericArrayType) {
			GenericArrayType t = (GenericArrayType) type;
			System.out.print("");
			printType(t.getGenericComponentType());
			System.out.print("[]");
		}

	}

};
