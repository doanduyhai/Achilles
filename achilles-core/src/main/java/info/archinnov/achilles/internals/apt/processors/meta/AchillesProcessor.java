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

package info.archinnov.achilles.internals.apt.processors.meta;

import static info.archinnov.achilles.internals.apt.AptUtils.*;
import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.MATERIALIZED_VIEW;
import static info.archinnov.achilles.internals.cassandra_version.CassandraFeature.UDF_UDA;
import static info.archinnov.achilles.internals.codegen.function.InternalSystemFunctionRegistry.SYSTEM_FUNCTIONS;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

import org.apache.commons.io.FileUtils;

import com.google.auto.service.AutoService;
import com.google.common.collect.Sets;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.cassandra_version.CassandraFeature;
import info.archinnov.achilles.internals.cassandra_version.InternalCassandraVersion;
import info.archinnov.achilles.internals.codegen.ManagerFactoryBuilderCodeGen;
import info.archinnov.achilles.internals.codegen.ManagerFactoryCodeGen;
import info.archinnov.achilles.internals.codegen.ManagerFactoryCodeGen.ManagersAndDSLClasses;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.parser.CodecRegistryParser;
import info.archinnov.achilles.internals.parser.EntityParser;
import info.archinnov.achilles.internals.parser.FunctionParser;
import info.archinnov.achilles.internals.parser.context.FunctionSignature;
import info.archinnov.achilles.internals.parser.context.FunctionsContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.utils.CollectionsHelper;


@AutoService(Processor.class)
public class AchillesProcessor extends AbstractProcessor {

    protected AptUtils aptUtils;
    protected EntityParser entityParser;
    private boolean processed = false;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);

        aptUtils = new AptUtils(processingEnv.getElementUtils(),
                processingEnv.getTypeUtils(), processingEnv.getMessager(),
                processingEnv.getFiler());
        entityParser = new EntityParser(aptUtils);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (!this.processed) {

            try {

                final GlobalParsingContext globalContext = initGlobalParsingContext(annotations, roundEnv);

                validateCassandraVersionAgainstUsedAnnotations(annotations, globalContext);

                parseCodecRegistry(globalContext, annotations, roundEnv);

                final List<EntityMetaSignature> tableAndViewSignatures = discoverAndValidateTablesAndViews(annotations, roundEnv, globalContext);

                final FunctionsContext udfContext = parseAndValidateFunctionRegistry(globalContext, annotations, roundEnv, tableAndViewSignatures);

                final TypeSpec managerFactoryBuilder = ManagerFactoryBuilderCodeGen.buildInstance(globalContext);

                final ManagersAndDSLClasses managersAndDSLClasses = ManagerFactoryCodeGen.buildInstance(aptUtils, tableAndViewSignatures, udfContext, globalContext);

                aptUtils.printNote("[Achilles] Reading previously generated source files (if exist)");
                try {
                    final FileObject resource = aptUtils.filer.getResource(StandardLocation.SOURCE_OUTPUT, GENERATED_PACKAGE, globalContext.managerFactoryBuilderClassName());
                    final File generatedSourceFolder = new File(resource.toUri().getRawPath().replaceAll("(.+/info/archinnov/achilles/generated/).+", "$1"));
                    aptUtils.printNote("[Achilles] Cleaning previously generated source files folder : '%s'", generatedSourceFolder.getPath());
                    FileUtils.deleteDirectory(generatedSourceFolder);
                } catch (IOException ioe) {
                    aptUtils.printNote("[Achilles] No previously generated source files found, proceed to code generation");
                }

                aptUtils.printNote("[Achilles] Generating CQL compatible types (used by the application) as class for function calls");
                for (TypeSpec typeSpec : globalContext.functionParameterTypesCodeGen().buildParameterTypesClasses(udfContext)) {
                    JavaFile.builder(FUNCTION_PACKAGE, typeSpec)
                            .build().writeTo(aptUtils.filer);
                }

                aptUtils.printNote("[Achilles] Generating SystemFunctions");
                JavaFile.builder(FUNCTION_PACKAGE, globalContext.functionsRegistryCodeGen().generateFunctionsRegistryClass(SYSTEM_FUNCTIONS_CLASS,
                        SYSTEM_FUNCTIONS)).build().writeTo(aptUtils.filer);

                if (globalContext.supportsFeature(UDF_UDA)) {
                    aptUtils.printNote("[Achilles] Generating FunctionsRegistry");
                    JavaFile.builder(FUNCTION_PACKAGE, globalContext.functionsRegistryCodeGen().generateFunctionsRegistryClass(FUNCTIONS_REGISTRY_CLASS,
                            udfContext.functionSignatures)).build().writeTo(aptUtils.filer);
                }


                aptUtils.printNote("[Achilles] Generating ManagerFactoryBuilder");
                JavaFile.builder(GENERATED_PACKAGE, managerFactoryBuilder)
                        .build().writeTo(aptUtils.filer);

                aptUtils.printNote("[Achilles] Generating Manager factory class");
                JavaFile.builder(GENERATED_PACKAGE, managersAndDSLClasses.managerFactoryClass)
                        .build().writeTo(aptUtils.filer);

                aptUtils.printNote("[Achilles] Generating UDT meta classes");
                for (TypeSpec typeSpec : globalContext.udtTypes.values()) {
                    JavaFile.builder(UDT_META_PACKAGE, typeSpec)
                            .build().writeTo(aptUtils.filer);
                }

                aptUtils.printNote("[Achilles] Generating entity meta classes");
                for (EntityMetaSignature signature : tableAndViewSignatures) {
                    JavaFile.builder(ENTITY_META_PACKAGE, signature.sourceCode)
                            .build().writeTo(aptUtils.filer);
                }

                aptUtils.printNote("[Achilles] Generating manager classes");
                for (TypeSpec manager : managersAndDSLClasses.managerClasses) {
                    JavaFile.builder(MANAGER_PACKAGE, manager)
                            .build().writeTo(aptUtils.filer);
                }

                aptUtils.printNote("[Achilles] Generating DSL classes");
                for (TypeSpec dsl : managersAndDSLClasses.dslClasses) {
                    JavaFile.builder(DSL_PACKAGE, dsl)
                            .build().writeTo(aptUtils.filer);
                }
            }catch (AchillesException e) {
                e.printStackTrace();
                aptUtils.printError("Error while parsing: %s", e.getMessage(), e);
            } catch (IllegalStateException e) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                aptUtils.printError("Error while parsing: %s", sw.toString(), e);
            } catch (IOException e) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                aptUtils.printError("Fail generating source file : %s", sw.toString(), e);

            } catch (Throwable throwable) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                throwable.printStackTrace(pw);
                aptUtils.printError("Fail generating source file : %s", sw.toString(), throwable);
            } finally {
                this.processed = true;
            }

        }
        return true;
    }

    private void validateCassandraVersionAgainstUsedAnnotations(Set<? extends TypeElement> annotations, GlobalParsingContext parsingContext) {
        final InternalCassandraVersion version = parsingContext.cassandraVersion;
        aptUtils.validateFalse(containsElementsAnnotatedBy(annotations, FunctionRegistry.class)
                && !version.supportsFeature(UDF_UDA),
                "Cassandra version %s does not support feature %s so @FunctionRegistry cannot be used",
                version.version(), UDF_UDA.name());

        aptUtils.validateFalse(containsElementsAnnotatedBy(annotations, MaterializedView.class)
                && !version.supportsFeature(MATERIALIZED_VIEW),
                "Cassandra version %s does not support feature %s so @MaterializedView cannot be used",
                version.version(), MATERIALIZED_VIEW.name());
    }

    private GlobalParsingContext initGlobalParsingContext(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        aptUtils.validateFalse(countElementsAnnotatedBy(annotations, CompileTimeConfig.class) > 1L,
                "Cannot declare more than one @%s in a single compilation unit",
                CompileTimeConfig.class.getSimpleName());

        GlobalParsingContext context = getTypesAnnotatedByAsStream(annotations, roundEnv, CompileTimeConfig.class)
                .map(typeElement -> aptUtils.getAnnotationOnClass(typeElement, CompileTimeConfig.class).get())
                .findFirst()
                .map(annot -> GlobalParsingContext.fromCompileTimeConfig(annot))
                .orElseGet(GlobalParsingContext::defaultContext);

        context.validateProjectName(aptUtils);

        return context;

    }

    private FunctionsContext parseAndValidateFunctionRegistry(GlobalParsingContext context, Set<? extends TypeElement> annotations,
                                                              RoundEnvironment roundEnv, List<EntityMetaSignature> tableAndViewSignatures) {

        final List<FunctionSignature> udfSignatures = getTypesAnnotatedByAsStream(annotations, roundEnv, FunctionRegistry.class)
                .flatMap(x -> FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, x, context).stream())
                .collect(toList());

        FunctionParser.validateNoDuplicateDeclaration(aptUtils, udfSignatures);

        final Set<TypeName> functionParameterTypes = udfSignatures
                .stream()
                .flatMap(x -> x.sourceParameterTypes.stream().map(TypeName::box))
                .collect(toSet());

        final Set<TypeName> functionReturnTypes = udfSignatures
                .stream()
                .map(x -> x.sourceReturnType.box())
                .collect(toSet());

        final Set<TypeName> entityColumnTargetTypes = tableAndViewSignatures
                .stream()
                .filter(EntityMetaSignature::isTable)
                .flatMap(x -> x.fieldMetaSignatures.stream())
                .map(x -> x.sourceType)
                .collect(toSet());

        return new FunctionsContext(udfSignatures, CollectionsHelper.appendAll(functionParameterTypes, functionReturnTypes, entityColumnTargetTypes, NATIVE_TYPES_2_1));
    }

    private List<EntityMetaSignature> discoverAndValidateTablesAndViews(Set<? extends TypeElement> annotatedTypes, RoundEnvironment roundEnv, GlobalParsingContext parsingContext) {
        final List<TypeElement> tableTypes = getTypesAnnotatedBy(annotatedTypes, roundEnv, Table.class);
        final List<TypeElement> viewTypes = getTypesAnnotatedBy(annotatedTypes, roundEnv, MaterializedView.class);
        final List<TypeElement> types = CollectionsHelper.appendAll(tableTypes, viewTypes);

        parsingContext.beanValidator().validateEntityNames(aptUtils, types);

        final List<EntityMetaSignature> tableSignatures = tableTypes
                .stream()
                .map(x -> entityParser.parseEntity(x, parsingContext))
                .collect(toList());

        final List<EntityMetaSignature> viewSignatures = parsingContext.supportsFeature(MATERIALIZED_VIEW)
                ? viewTypes
                    .stream()
                    .map(x -> entityParser.parseView(x, parsingContext))
                    .collect(toList())
                : Collections.EMPTY_LIST;

        final List<EntityMetaSignature> tableAndViewSignatures = CollectionsHelper.appendAll(tableSignatures, viewSignatures);

        if (parsingContext.supportsFeature(CassandraFeature.MATERIALIZED_VIEW)) {
            parsingContext.beanValidator().validateViewsAgainstBaseTable(aptUtils, viewSignatures, tableSignatures);
        }

        return tableAndViewSignatures;
    }

    private void parseCodecRegistry(GlobalParsingContext parsingContext, Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (containsElementsAnnotatedBy(annotations, CodecRegistry.class)) {
            aptUtils.printNote("[Achilles] Parsing compile-time codec registry");
            new CodecRegistryParser(aptUtils).parseCodecs(roundEnv, parsingContext);
        }
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Sets.newHashSet(Table.class.getCanonicalName(),
                MaterializedView.class.getCanonicalName(),
                CodecRegistry.class.getCanonicalName(),
                FunctionRegistry.class.getCanonicalName(),
                CompileTimeConfig.class.getCanonicalName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

}
