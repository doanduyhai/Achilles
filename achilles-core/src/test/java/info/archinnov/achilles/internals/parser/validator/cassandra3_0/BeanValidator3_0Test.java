/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.internals.parser.validator.cassandra3_0;

import static info.archinnov.achilles.internals.codegen.TypeParsingResultConsumer.getTypeParsingResults;
import static info.archinnov.achilles.internals.metamodel.AbstractEntityProperty.EntityType.TABLE;
import static info.archinnov.achilles.internals.metamodel.AbstractEntityProperty.EntityType.VIEW;
import static java.util.Collections.emptyList;

import java.util.Arrays;
import javax.lang.model.element.TypeElement;

import org.junit.Test;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.parser.validator.BeanValidator;
import info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityAsChild;
import info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntitySensor;
import info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntitySensorWithCollection;
import info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithCompositePartitionKey;
import info.archinnov.achilles.internals.sample_classes.parser.view.*;

public class BeanValidator3_0Test extends AbstractTestProcessor {

    private GlobalParsingContext context = GlobalParsingContext.defaultContext();
    private BeanValidator beanValidator = new BeanValidator3_0();

    @Test
    public void should_validate_view_against_base_table() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElementBase = aptUtils.elementUtils.getTypeElement(TestEntitySensor.class.getCanonicalName());
            final TypeElement typeElementView = aptUtils.elementUtils.getTypeElement(TestViewSensorByType.class.getCanonicalName());

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final EntityMetaCodeGen.EntityMetaSignature baseSignature = builder.buildEntityMeta(TABLE, typeElementBase, context, getTypeParsingResults(aptUtils, typeElementBase, context), emptyList());
            final EntityMetaCodeGen.EntityMetaSignature viewSignature = builder.buildEntityMeta(VIEW, typeElementView, context, getTypeParsingResults(aptUtils, typeElementView, context), emptyList());

            beanValidator.validateViewsAgainstBaseTable(aptUtils, Arrays.asList(viewSignature), Arrays.asList(baseSignature));
        });
        launchTest(TestViewSensorByType.class);
    }

    @Test
    public void should_fail_validating_view_without_base_entity() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElementBase = aptUtils.elementUtils.getTypeElement(TestEntityAsChild.class.getCanonicalName());
            final TypeElement typeElementView = aptUtils.elementUtils.getTypeElement(TestViewSensorByType.class.getCanonicalName());

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final EntityMetaCodeGen.EntityMetaSignature baseSignature = builder.buildEntityMeta(TABLE, typeElementBase, context, getTypeParsingResults(aptUtils, typeElementBase, context), emptyList());
            final EntityMetaCodeGen.EntityMetaSignature viewSignature = builder.buildEntityMeta(VIEW, typeElementView, context, getTypeParsingResults(aptUtils, typeElementView, context), emptyList());

            beanValidator.validateViewsAgainstBaseTable(aptUtils, Arrays.asList(viewSignature), Arrays.asList(baseSignature));
        });
        failTestWithMessage("Cannot find base entity class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntitySensor' for view class 'info.archinnov.achilles.internals.sample_classes.parser.view.TestViewSensorByType'", TestViewSensorByType.class);
    }

    @Test
    public void should_fail_validating_view_because_column_not_in_base() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElementBase = aptUtils.elementUtils.getTypeElement(TestEntityWithCompositePartitionKey.class.getCanonicalName());
            final TypeElement typeElementView = aptUtils.elementUtils.getTypeElement(TestViewSensorByType.class.getCanonicalName());

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final EntityMetaCodeGen.EntityMetaSignature baseSignature = builder.buildEntityMeta(TABLE, typeElementBase, context, getTypeParsingResults(aptUtils, typeElementBase, context), emptyList());
            final EntityMetaCodeGen.EntityMetaSignature viewSignature = builder.buildEntityMeta(VIEW, typeElementView, context, getTypeParsingResults(aptUtils, typeElementView, context), emptyList());

            beanValidator.validateViewsAgainstBaseTable(aptUtils, Arrays.asList(viewSignature), Arrays.asList(baseSignature));
        });
        failTestWithMessage("Cannot find base entity class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntitySensor' for view class 'info.archinnov.achilles.internals.sample_classes.parser.view.TestViewSensorByType'", TestViewSensorByType.class);
    }

    @Test
    public void should_fail_validating_view_because_column_not_correct_type() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElementBase = aptUtils.elementUtils.getTypeElement(TestEntitySensor.class.getCanonicalName());
            final TypeElement typeElementView = aptUtils.elementUtils.getTypeElement(TestViewSensorWithColumnNotMatchBase.class.getCanonicalName());

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final EntityMetaCodeGen.EntityMetaSignature baseSignature = builder.buildEntityMeta(TABLE, typeElementBase, context, getTypeParsingResults(aptUtils, typeElementBase, context), emptyList());
            final EntityMetaCodeGen.EntityMetaSignature viewSignature = builder.buildEntityMeta(VIEW, typeElementView, context, getTypeParsingResults(aptUtils, typeElementView, context), emptyList());

            beanValidator.validateViewsAgainstBaseTable(aptUtils, Arrays.asList(viewSignature), Arrays.asList(baseSignature));
        });
        failTestWithMessage("Cannot find any match in base table for field '{fieldName='value', cqlColumn='value', sourceType=java.lang.Long, targetType=java.lang.Long}' in view class 'info.archinnov.achilles.internals.sample_classes.parser.view.TestViewSensorWithColumnNotMatchBase'", TestEntitySensor.class);
    }

    @Test
    public void should_fail_validating_view_because_missing_base_pk() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElementBase = aptUtils.elementUtils.getTypeElement(TestEntitySensor.class.getCanonicalName());
            final TypeElement typeElementView = aptUtils.elementUtils.getTypeElement(TestViewSensorWithMissingPK.class.getCanonicalName());

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final EntityMetaCodeGen.EntityMetaSignature baseSignature = builder.buildEntityMeta(TABLE, typeElementBase, context, getTypeParsingResults(aptUtils, typeElementBase, context), emptyList());
            final EntityMetaCodeGen.EntityMetaSignature viewSignature = builder.buildEntityMeta(VIEW, typeElementView, context, getTypeParsingResults(aptUtils, typeElementView, context), emptyList());

            beanValidator.validateViewsAgainstBaseTable(aptUtils, Arrays.asList(viewSignature), Arrays.asList(baseSignature));
        });
        failTestWithMessage("Primary key column '{fieldName='date', cqlColumn='date', sourceType=java.util.Date, targetType=java.util.Date}' in base class info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntitySensor is not found in view class 'info.archinnov.achilles.internals.sample_classes.parser.view.TestViewSensorWithMissingPK' as primary key column", TestEntitySensor.class);
    }

    @Test
    public void should_fail_validating_view_because_missing_base_collection() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElementBase = aptUtils.elementUtils.getTypeElement(TestEntitySensorWithCollection.class.getCanonicalName());
            final TypeElement typeElementView = aptUtils.elementUtils.getTypeElement(TestViewSensorWithMissingCollection.class.getCanonicalName());

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final EntityMetaCodeGen.EntityMetaSignature baseSignature = builder.buildEntityMeta(TABLE, typeElementBase, context, getTypeParsingResults(aptUtils, typeElementBase, context), emptyList());
            final EntityMetaCodeGen.EntityMetaSignature viewSignature = builder.buildEntityMeta(VIEW, typeElementView, context, getTypeParsingResults(aptUtils, typeElementView, context), emptyList());

            beanValidator.validateViewsAgainstBaseTable(aptUtils, Arrays.asList(viewSignature), Arrays.asList(baseSignature));
        });
        failTestWithMessage("Collection/UDT column '{fieldName='values', cqlColumn='values', sourceType=java.util.List<java.lang.Double>, targetType=java.util.List<java.lang.Double>}' in base class info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntitySensorWithCollection is not found in view class 'info.archinnov.achilles.internals.sample_classes.parser.view.TestViewSensorWithMissingCollection'. It should be included in the view", TestEntitySensorWithCollection.class);
    }

    @Test
    public void should_fail_validating_view_because_more_than_1_non_pk() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElementBase = aptUtils.elementUtils.getTypeElement(TestEntitySensor.class.getCanonicalName());
            final TypeElement typeElementView = aptUtils.elementUtils.getTypeElement(TestViewSensorWithMultipleNonPK.class.getCanonicalName());

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final EntityMetaCodeGen.EntityMetaSignature baseSignature = builder.buildEntityMeta(TABLE, typeElementBase, context, getTypeParsingResults(aptUtils, typeElementBase, context), emptyList());
            final EntityMetaCodeGen.EntityMetaSignature viewSignature = builder.buildEntityMeta(VIEW, typeElementView, context, getTypeParsingResults(aptUtils, typeElementView, context), emptyList());

            beanValidator.validateViewsAgainstBaseTable(aptUtils, Arrays.asList(viewSignature), Arrays.asList(baseSignature));
        });
        failTestWithMessage("There should be maximum 1 column in the view info.archinnov.achilles.internals.sample_classes.parser.view.TestViewSensorWithMultipleNonPK primary key that is NOT a primary column of the base class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntitySensor'. We have [{fieldName='type', cqlColumn='type', sourceType=java.lang.String, targetType=java.lang.String}, {fieldName='value', cqlColumn='value', sourceType=java.lang.Double, targetType=java.lang.Double}]", TestEntitySensor.class);
    }
}