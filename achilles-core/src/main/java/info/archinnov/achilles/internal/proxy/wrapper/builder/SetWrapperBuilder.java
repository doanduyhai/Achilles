/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.proxy.wrapper.builder;

import java.util.Set;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.proxy.wrapper.SetWrapper;

public class SetWrapperBuilder extends AbstractWrapperBuilder<SetWrapperBuilder> {
	private Set<Object> target;

	public static SetWrapperBuilder builder(PersistenceContext context, Set<Object> target) {
		return new SetWrapperBuilder(context, target);
	}

	public SetWrapperBuilder(PersistenceContext context, Set<Object> target) {
		super.context = context;
		this.target = target;
	}

	public SetWrapper build() {
		SetWrapper setWrapper = new SetWrapper(this.target);
		super.build(setWrapper);
		return setWrapper;
	}

}
