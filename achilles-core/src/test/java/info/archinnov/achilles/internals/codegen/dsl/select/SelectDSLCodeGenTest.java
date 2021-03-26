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

package info.archinnov.achilles.internals.codegen.dsl.select;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Optional;
import javax.lang.model.element.TypeElement;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.squareup.javapoet.JavaFile;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.cassandra_version.V2_1;
import info.archinnov.achilles.internals.codegen.dsl.select.cassandra2_1.SelectDSLCodeGen2_1;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.parser.EntityParser;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.dsl.select.TestEntityWithUDTAsClustering;
import info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter;
import info.archinnov.achilles.internals.strategy.naming.LowerCaseNaming;
import info.archinnov.achilles.type.strategy.InsertStrategy;

public class SelectDSLCodeGenTest extends AbstractTestProcessor {

    @Test
    public void should_generate_select_dsl_class_for_udt_as_clustering() throws Exception {
        setExec(aptUtils -> {

            final GlobalParsingContext globalContext = new GlobalParsingContext(
                    V2_1.INSTANCE,
                    InsertStrategy.ALL_FIELDS,
                    new LowerCaseNaming(),
                    FieldFilter.EXPLICIT_ENTITY_FIELD_FILTER,
                    FieldFilter.EXPLICIT_UDT_FIELD_FILTER,
                    Optional.empty());

            final String className = TestEntityWithUDTAsClustering.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityParser entityParser = new EntityParser(aptUtils);

            final EntityMetaCodeGen.EntityMetaSignature entityMetaSignature = entityParser.parseEntity(typeElement, globalContext);

            final SelectDSLCodeGen2_1 selectDSLCodeGen2_1 = new SelectDSLCodeGen2_1();

            final StringBuilder builder = new StringBuilder();
            try {
                JavaFile.builder(TypeUtils.GENERATED_PACKAGE, selectDSLCodeGen2_1.buildSelectClass(globalContext, entityMetaSignature))
                        .build()
                        .writeTo(builder);
            } catch (IOException e) {
                Assertions.assertThat(false).as("IOException when generating class : %s", e.getMessage()).isTrue();
            }

            assertThat(builder.toString().trim()).isEqualTo(
                    readCodeBlockFromFile("expected_code/dsl/select/should_generate_manager_class_for_index_dsl.txt", false));
        });
        launchTest();
    }
}