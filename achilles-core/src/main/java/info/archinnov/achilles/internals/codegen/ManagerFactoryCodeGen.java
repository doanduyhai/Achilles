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

package info.archinnov.achilles.internals.codegen;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.ManagerCodeGen.ManagerAndDSLClasses;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.context.FunctionSignature;
import info.archinnov.achilles.internals.parser.context.FunctionSignature.FunctionParamSignature;
import info.archinnov.achilles.internals.parser.context.FunctionsContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public class ManagerFactoryCodeGen {

    public static ManagersAndDSLClasses buildInstance(AptUtils aptUtils, List<EntityMetaSignature> signatures, FunctionsContext functionsContext,
                                                      GlobalParsingContext parsingContext) {
        List<TypeSpec> managerClasses = new ArrayList<>();
        List<TypeSpec> dslClasses = new ArrayList<>();
        final TypeSpec.Builder builder = TypeSpec.classBuilder(parsingContext.managerFactoryClassName())
                .superclass(ABSTRACT_MANAGER_FACTORY)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(buildConstructor(signatures, functionsContext))
                .addMethod(buildGetCassandraVersion(parsingContext));

        for(EntityMetaSignature x: signatures) {
            TypeName managerType = ClassName.get(MANAGER_PACKAGE, x.className + MANAGER_SUFFIX);
            final ManagerAndDSLClasses managerAndDSLClasses = ManagerCodeGen.buildManager(parsingContext, aptUtils, x);
            managerClasses.add(managerAndDSLClasses.managerClass);
            dslClasses.addAll(managerAndDSLClasses.dslClasses);
            final FieldSpec entityPropertyMeta = FieldSpec
                    .builder(x.typeName, x.fieldName + META_SUFFIX, Modifier.FINAL, Modifier.PRIVATE)
                    .initializer("new $T()", x.typeName)
                    .build();

            final FieldSpec entityManager = FieldSpec
                    .builder(managerType, x.fieldName + MANAGER_SUFFIX, Modifier.FINAL, Modifier.PRIVATE)
                    .initializer("new $T($T.class, $L, rte)", managerType, x.entityRawClass, x.fieldName + META_SUFFIX)
                    .build();

            builder.addField(entityPropertyMeta)
                    .addField(entityManager)
                    .addMethod(buildManagerFor(x));
        }

        for (FunctionSignature functionSignature : functionsContext.functionSignatures) {
            builder.addField(buildFunctionProperty(functionSignature));
        }

        TypeName listOfUdtClassProperties = genericType(LIST, genericType(ABSTRACT_UDT_CLASS_PROPERTY, WILDCARD));

        builder.addMethod(getUDTClassProperties(parsingContext, listOfUdtClassProperties));

        return new ManagersAndDSLClasses(builder.build(), managerClasses, dslClasses);
    }

    private static FieldSpec buildFunctionProperty(FunctionSignature functionSignature) {
        final String fieldName = functionSignature.name + FUNCTION_PROPERTY_SUFFIX;
        CodeBlock keyspaceCodeBlock;
        if (functionSignature.keyspace.isPresent()) {
            keyspaceCodeBlock = CodeBlock.builder().add("$T.of($S)", OPTIONAL, functionSignature.keyspace.get()).build();
        } else {
            keyspaceCodeBlock = CodeBlock.builder().add("$T.empty()", OPTIONAL).build();
        }
        StringJoiner functionParams = new StringJoiner(", ", "$T.asList(", ")");

        for (FunctionParamSignature parameterSignature : functionSignature.parameterSignatures) {
            functionParams.add("\"" + parameterSignature.targetCQLDataType + "\"");
        }

        CodeBlock parametersCodeBlock = CodeBlock.builder().add(functionParams.toString(), ARRAYS).build();

        CodeBlock functionProperty = CodeBlock
                .builder()
                .add("new $T($L, $S, $S, $L)", FUNCTION_PROPERTY,
                        keyspaceCodeBlock,
                        functionSignature.name.toLowerCase(),
                        functionSignature.returnTypeSignature.targetCQLDataType,
                        parametersCodeBlock)
                .build();

        return FieldSpec.builder(FUNCTION_PROPERTY, fieldName, Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer(functionProperty)
                .build();
    }

    private static MethodSpec buildGetCassandraVersion(GlobalParsingContext parsingContext) {

        ClassName cassandraVersionClass = ClassName.get(parsingContext.cassandraVersion.getClass());
        return MethodSpec
                .methodBuilder("getCassandraVersion")
                .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
                .addAnnotation(Override.class)
                .returns(INTERNAL_CASSANDRA_VERSION)
                .addStatement("return $T.INSTANCE", cassandraVersionClass)
                .build();
    }

    private static MethodSpec getUDTClassProperties(GlobalParsingContext parsingContext, TypeName listOfUdtClassProperties) {
        final MethodSpec.Builder getUdtClassPropertiesBuilder = MethodSpec
                .methodBuilder("getUdtClassProperties")
                .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
                .addAnnotation(Override.class)
                .returns(listOfUdtClassProperties)
                .addStatement("final $T list = new $T<>()", listOfUdtClassProperties, TypeUtils.ARRAY_LIST);

        for(Map.Entry<TypeName, TypeSpec> entry: parsingContext.udtTypes.entrySet()) {
            getUdtClassPropertiesBuilder.addStatement("list.add($L.INSTANCE)", TypeUtils.UDT_META_PACKAGE
                    + "." + entry.getValue().name);
        }

        getUdtClassPropertiesBuilder.addStatement("return list");
        return getUdtClassPropertiesBuilder.build();
    }

    private static MethodSpec buildConstructor(List<EntityMetaSignature> signatures, FunctionsContext functionsContext) {
        final StringJoiner entityProperties = new StringJoiner(", ");
        final StringJoiner functionProperties = new StringJoiner(", ");

        signatures
                .stream()
                .map(x -> x.fieldName + META_SUFFIX)
                .forEach(entityProperties::add);

        functionsContext.functionSignatures
                .stream()
                .map(signature -> signature.name + FUNCTION_PROPERTY_SUFFIX)
                .forEach(functionProperties::add);

        return MethodSpec
                .constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(CLUSTER, "cluster", Modifier.FINAL)
                .addParameter(CONFIGURATION_CONTEXT, "configContext", Modifier.FINAL)
                .addStatement("super($N, $N)", "cluster", "configContext")
                .addStatement("this.entityProperties = $T.asList($L)", ARRAYS, entityProperties.toString())
                .addStatement("this.functionProperties = $T.asList($L)", ARRAYS, functionProperties.toString())
                .addStatement("this.entityClasses = this.entityProperties.stream().map(x -> x.entityClass).collect($T.toList())", COLLECTORS)
                .addStatement("bootstrap()")
                .build();
    }


    private static MethodSpec buildManagerFor(EntityMetaSignature signature) {
        TypeName returnType = ClassName.get(MANAGER_PACKAGE, signature.className + MANAGER_SUFFIX);
        return MethodSpec
                .methodBuilder("for" + signature.className)
                .addJavadoc("Create a Manager for entity class $T", signature.entityRawClass)
                .addJavadoc("\n")
                .addJavadoc("@return $T", returnType)
                .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
                .addStatement("return $L", signature.fieldName + MANAGER_SUFFIX)
                .returns(returnType)
                .build();

    }

    public static class ManagersAndDSLClasses {
        public final TypeSpec managerFactoryClass;
        public final List<TypeSpec> managerClasses;
        public final List<TypeSpec> dslClasses;

        public ManagersAndDSLClasses(TypeSpec managerFactoryClass, List<TypeSpec> managerClasses, List<TypeSpec> dslClasses) {
            this.managerFactoryClass = managerFactoryClass;
            this.managerClasses = managerClasses;
            this.dslClasses = dslClasses;
        }
    }
}
