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
package info.archinnov.achilles.entity.parsing.validator;

import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ConfigurationContext.Impl;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

public class EntityParsingValidatorTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private EntityParsingValidator validator = new EntityParsingValidator();

	@Test
	public void should_exception_when_no_id_meta() throws Exception {
		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("The entity '"
						+ CompleteBean.class.getCanonicalName()
						+ "' should have at least one field with javax.persistence.Id/javax.persistence.EmbeddedId annotation");
		validator.validateHasIdMeta(CompleteBean.class, null);
	}

	@Test
	public void should_exception_when_value_less_property_meta_map()
			throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).build();

		EntityParsingContext context = new EntityParsingContext(null,
				CompleteBean.class);
		context.setPropertyMetas(ImmutableMap.<String, PropertyMeta> of("id",
				idMeta));
		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("The entity '"
						+ CompleteBean.class.getCanonicalName()
						+ "' should have at least one field with javax.persistence.Column/javax.persistence.Id/javax.persistence.EmbeddedId annotations");

		validator.validatePropertyMetas(context, idMeta);
	}

	@Test
	public void should_skip_wide_row_validation_when_not_wide_row_with_thrift_impl()
			throws Exception {
		EntityParsingContext context = new EntityParsingContext(null,
				CompleteBean.class);
		context.setClusteredEntity(false);
		context.setPropertyMetas(new HashMap<String, PropertyMeta>());

		validator.validateClusteredEntities(context);
	}

	@Test
	public void should_skip_wide_row_validation_when_not_wide_row_with_cql_impl()
			throws Exception {
		ConfigurationContext configContext = new ConfigurationContext();
		configContext.setImpl(Impl.CQL);
		EntityParsingContext context = new EntityParsingContext(configContext,
				CompleteBean.class);

		context.setClusteredEntity(false);
		context.setPropertyMetas(new HashMap<String, PropertyMeta>());

		validator.validateClusteredEntities(context);
	}

	@Test
	public void should_skip_wide_row_validation_for_wide_row_but_with_cql_impl()
			throws Exception {
		ConfigurationContext configContext = new ConfigurationContext();
		configContext.setImpl(Impl.CQL);
		EntityParsingContext context = new EntityParsingContext(configContext,
				CompleteBean.class);

		context.setClusteredEntity(true);
		context.setPropertyMetas(new HashMap<String, PropertyMeta>());

		validator.validateClusteredEntities(context);
	}

	@Test
	public void should_exception_when_more_than_two_property_metas_for_wide_row()
			throws Exception {
		ConfigurationContext configContext = new ConfigurationContext();
		configContext.setImpl(Impl.THRIFT);
		EntityParsingContext context = new EntityParsingContext(configContext,
				CompleteBean.class);
		HashMap<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("name", null);
		propertyMetas.put("age", null);
		propertyMetas.put("id", null);
		context.setPropertyMetas(propertyMetas);
		context.setClusteredEntity(true);

		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("The clustered entity '"
						+ CompleteBean.class.getCanonicalName()
						+ "' should not have more than two properties annotated with @EmbeddedId/@Id/@Column");

		validator.validateClusteredEntities(context);
	}

	@Test
	public void should_exception_when_no_embedded_id_found_for_wide_row()
			throws Exception {
		ConfigurationContext configContext = new ConfigurationContext();
		configContext.setImpl(Impl.THRIFT);
		EntityParsingContext context = new EntityParsingContext(configContext,
				CompleteBean.class);
		HashMap<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();

		PropertyMeta idMeta = PropertyMetaTestBuilder //
				.valueClass(Long.class).type(PropertyType.ID).build();

		PropertyMeta propertyMeta = PropertyMetaTestBuilder //
				.valueClass(String.class).type(PropertyType.SIMPLE).build();
		propertyMetas.put("id", idMeta);
		propertyMetas.put("name", propertyMeta);
		context.setPropertyMetas(propertyMetas);
		context.setClusteredEntity(true);

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The clustered entity '"
				+ CompleteBean.class.getCanonicalName()
				+ "' should have an @EmbeddedId");

		validator.validateClusteredEntities(context);
	}

	@Test
	public void should_exception_when_incorrect_clustered_value_type_for_wide_row()
			throws Exception {
		ConfigurationContext configContext = new ConfigurationContext();
		configContext.setImpl(Impl.THRIFT);
		EntityParsingContext context = new EntityParsingContext(configContext,
				CompleteBean.class);
		HashMap<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();

		PropertyMeta idMeta = PropertyMetaTestBuilder //
				.valueClass(Long.class).type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta propertyMeta = PropertyMetaTestBuilder //
				.valueClass(String.class).type(PropertyType.LIST).build();
		propertyMetas.put("id", idMeta);
		propertyMetas.put("name", propertyMeta);
		context.setPropertyMetas(propertyMetas);
		context.setClusteredEntity(true);

		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("The clustered entity '"
						+ CompleteBean.class.getCanonicalName()
						+ "' should have a single @Column property of type simple/counter");

		validator.validateClusteredEntities(context);
	}

	@Test
	public void should_exception_when_no_entity_found_after_parsing()
			throws Exception {
		List<Class<?>> entities = new ArrayList<Class<?>>();
		List<String> entityPackages = Arrays.asList("com.package1",
				"com.package2");

		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages 'com.package1,com.package2'");

		validator.validateAtLeastOneEntity(entities, entityPackages);

	}
}
