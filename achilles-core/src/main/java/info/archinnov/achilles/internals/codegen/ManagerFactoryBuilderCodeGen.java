/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

package info.archinnov.achilles.internals.codegen;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

public class ManagerFactoryBuilderCodeGen {

    public static TypeSpec buildInstance() {

        return TypeSpec.classBuilder(MANAGER_FACTORY_BUILDER_CLASS)
                .superclass(genericType(ABSTRACT_MANAGER_FACTORY_BUILDER, MANAGER_FACTORY_BUILDER))
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                        // private ManagerFactoryBuilder(Cluster cluster) {super(cluster);}
                .addMethod(MethodSpec
                        .constructorBuilder()
                        .addModifiers(Modifier.PRIVATE)
                        .addParameter(CLUSTER, "cluster", Modifier.FINAL)
                        .addStatement("super(cluster)")
                        .build())
                        // protected  T getThis() {return this};
                .addMethod(MethodSpec
                        .methodBuilder("getThis")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PROTECTED)
                        .addStatement("return this")
                        .returns(MANAGER_FACTORY_BUILDER)
                        .build())
                /*
                    public static ManagerFactoryBuilder builder(Cluster cluster) {
                        return new ManagerFactoryBuilder(cluster);
                    }
                 */
                .addMethod(MethodSpec.methodBuilder("builder")
                        .addJavadoc("Create a @{link $T} instance", MANAGER_FACTORY_BUILDER)
                        .addJavadoc("@param cluster native @{link $T} object", CLUSTER)
                        .addJavadoc("@return $T", MANAGER_FACTORY_BUILDER)
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .addParameter(CLUSTER, "cluster")
                        .addStatement("return new $T($N)", MANAGER_FACTORY_BUILDER, "cluster")
                        .returns(MANAGER_FACTORY_BUILDER)
                        .build())
                /*
                    public static ManagerFactory build(Cluster cluster, Map<ConfigurationParameters, Object> configurationMap) {
                        return new ManagerFactory(cluster, configurationMap).bootstrap();
                    }
                 */
                .addMethod(MethodSpec.methodBuilder("build")
                        .addJavadoc("Build a @{link $T} instance", MANAGER_FACTORY)
                        .addJavadoc("@param cluster native @{link $T} object", CLUSTER)
                        .addJavadoc("@param configurationMap Achilles configuration map")
                        .addJavadoc("@return $T", MANAGER_FACTORY)
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(CLUSTER, "cluster")
                        .addParameter(genericType(MAP, CONFIGURATION_PARAMETERS, TypeName.OBJECT), "configurationMap")
                        .addStatement("return new $T($N, buildConfigContext($N, $T.fromMap($N)))", MANAGER_FACTORY,
                                "cluster", "cluster", CONFIG_MAP, "configurationMap")
                        .returns(MANAGER_FACTORY)
                        .build())
                /*
                    public PersistenceManagerFactory build() {
                        return new PersistenceManagerFactory(cluster, configMap).bootstrap();
                    }
                 */
                .addMethod(MethodSpec.methodBuilder("build")
                        .addJavadoc("Build a @{link $T} instance", MANAGER_FACTORY)
                        .addJavadoc("@return $T", MANAGER_FACTORY)
                        .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "unchecked").build())
                        .addModifiers(Modifier.PUBLIC)
                        .addStatement("return new $T(this.$L, buildConfigContext(this.$L, this.$L))", MANAGER_FACTORY,
                                "cluster", "cluster", "configMap")
                        .returns(MANAGER_FACTORY)
                        .build())
                .build();
    }
}
