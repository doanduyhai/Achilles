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
package info.archinnov.achilles.compound;

import java.util.Comparator;
import java.util.List;

public abstract class CompoundKeyValidator {
	protected ComponentComparator comparator = new ComponentComparator();

	public int getLastNonNullIndex(List<Object> components) {
		for (int i = 0; i < components.size(); i++) {
			if (components.get(i) == null) {
				return i - 1;
			}
		}
		return components.size() - 1;
	}

	protected static class ComponentComparator implements Comparator<Object> {

		@SuppressWarnings("unchecked")
		@Override
		public int compare(Object o1, Object o2) {
			if (o1.getClass().isEnum() && o2.getClass().isEnum()) {
				String name1 = ((Enum<?>) o1).name();
				String name2 = ((Enum<?>) o2).name();

				return name1.compareTo(name2);
			} else if (Comparable.class.isAssignableFrom(o1.getClass())
					&& Comparable.class.isAssignableFrom(o2.getClass())) {
				Comparable<Object> comp1 = (Comparable<Object>) o1;
				Comparable<Object> comp2 = (Comparable<Object>) o2;

				return comp1.compareTo(comp2);
			} else {
				throw new IllegalArgumentException("Type '" + o1.getClass().getCanonicalName() + "' or type '"
						+ o2.getClass().getCanonicalName() + "' should implements Comparable");
			}
		}
	}
}
