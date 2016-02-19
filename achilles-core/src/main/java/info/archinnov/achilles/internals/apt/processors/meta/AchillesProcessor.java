/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

import static info.archinnov.achilles.internals.apt.AptUtils.isAnnotationOfType;
import static info.archinnov.achilles.internals.metamodel.functions.SystemFunctionRegistry.SYSTEM_FUNCTIONS;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.parser.validator.BeanValidator.validateViewsAgainstBaseTable;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;

import com.google.auto.common.MoreElements;
import com.google.auto.service.AutoService;
import com.google.common.collect.Sets;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.ManagerFactoryBuilderCodeGen;
import info.archinnov.achilles.internals.codegen.ManagerFactoryCodeGen;
import info.archinnov.achilles.internals.codegen.ManagerFactoryCodeGen.ManagersAndDSLClasses;
import info.archinnov.achilles.internals.codegen.function.FunctionParameterTypesCodeGen;
import info.archinnov.achilles.internals.codegen.function.FunctionsRegistryCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.functions.SystemFunctionRegistry;
import info.archinnov.achilles.internals.metamodel.functions.UDFSignature;
import info.archinnov.achilles.internals.parser.CodecRegistryParser;
import info.archinnov.achilles.internals.parser.EntityParser;
import info.archinnov.achilles.internals.parser.UDFParser;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.context.UDFContext;
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
                final GlobalParsingContext parsingContext = parseCodecRegistry(annotations, roundEnv);

                final List<EntityMetaSignature> tableAndViewSignatures = discoverAndValidateTablesAndViews(annotations, roundEnv, parsingContext);

                final UDFContext udfContext = parseAndValidateFunctionRegistry(annotations, roundEnv, tableAndViewSignatures);
                final List<UDFSignature> systemFunctionsSignature = CollectionsHelper.appendAll(SYSTEM_FUNCTIONS,
                        udfContext.buildExtraUDFSignatureForSystemFunctionsAcceptingAnyType());

                final TypeSpec managerFactoryBuilder = ManagerFactoryBuilderCodeGen.buildInstance();
                final ManagersAndDSLClasses managersAndDSLClasses = ManagerFactoryCodeGen.buildInstance(aptUtils, tableAndViewSignatures, parsingContext);

                aptUtils.printNote("[Achilles] Reading previously generated source files (if exist)");
                try {
                    final FileObject resource = aptUtils.filer.getResource(StandardLocation.SOURCE_OUTPUT, GENERATED_PACKAGE, MANAGER_FACTORY_BUILDER_CLASS);
                    final File generatedSourceFolder = new File(resource.toUri().getRawPath().replaceAll("(.+/info/archinnov/achilles/generated/).+", "$1"));
                    aptUtils.printNote("[Achilles] Cleaning previously generated source files");
                    FileUtils.deleteDirectory(generatedSourceFolder);
                } catch (IOException ioe) {
                    aptUtils.printNote("[Achilles] No previously generated source files found, proceed to code generation");
                }

                aptUtils.printNote("[Achilles] Generating CQL compatible types (used by the application) as class");
                for (TypeSpec typeSpec : FunctionParameterTypesCodeGen.buildParameterTypesClasses(udfContext)) {
                    JavaFile.builder(FUNCTION_PACKAGE, typeSpec)
                            .build().writeTo(aptUtils.filer);
                }

                aptUtils.printNote("[Achilles] Generating SystemFunctions");
                JavaFile.builder(FUNCTION_PACKAGE, FunctionsRegistryCodeGen.generateFunctionsRegistryClass(SYSTEM_FUNCTIONS_CLASS,
                        systemFunctionsSignature)).build().writeTo(aptUtils.filer);

                aptUtils.printNote("[Achilles] Generating FunctionsRegistry");
                JavaFile.builder(FUNCTION_PACKAGE, FunctionsRegistryCodeGen.generateFunctionsRegistryClass(FUNCTIONS_REGISTRY_CLASS,
                        udfContext.udfSignatures)).build().writeTo(aptUtils.filer);


                aptUtils.printNote("[Achilles] Generating ManagerFactoryBuilder");
                JavaFile.builder(GENERATED_PACKAGE, managerFactoryBuilder)
                        .build().writeTo(aptUtils.filer);

                aptUtils.printNote("[Achilles] Generating Manager factory class");
                JavaFile.builder(GENERATED_PACKAGE, managersAndDSLClasses.managerFactoryClass)
                        .build().writeTo(aptUtils.filer);

                aptUtils.printNote("[Achilles] Generating UDT meta classes");
                for (TypeSpec typeSpec : parsingContext.udtTypes.values()) {
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

    private UDFContext parseAndValidateFunctionRegistry(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv, List<EntityMetaSignature> tableAndViewSignatures) {
        final List<UDFSignature> udfSignatures = annotations
                .stream()
                .filter(annotation -> isAnnotationOfType(annotation, FunctionRegistry.class))
                .flatMap(annotation -> roundEnv.getElementsAnnotatedWith(annotation).stream())
                .map(MoreElements::asType)
                .flatMap(x -> UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, x).stream())
                .collect(toList());

        UDFParser.validateNoDuplicateDeclaration(aptUtils, udfSignatures);

        final Set<TypeName> functionParameterTypes = udfSignatures
                .stream()
                .flatMap(x -> x.parameterTypes.stream().map(TypeName::box))
                .collect(toSet());

        final Set<TypeName> entityColumnTargetTypes = tableAndViewSignatures
                .stream()
                .filter(EntityMetaSignature::isTable)
                .flatMap(x -> x.parsingResults.stream())
                .map(x -> x.targetType)
                .map(TypeUtils::mapToNativeCassandraType)
                .collect(toSet());

        return new UDFContext(udfSignatures, CollectionsHelper.appendAll(functionParameterTypes, entityColumnTargetTypes, NATIVE_TYPES));
    }

    private List<EntityMetaSignature> discoverAndValidateTablesAndViews(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv, GlobalParsingContext parsingContext) {
        final List<TypeElement> tableTypes = annotations
                .stream()
                .filter(annotation -> isAnnotationOfType(annotation, Table.class))
                .flatMap(annotation -> roundEnv.getElementsAnnotatedWith(annotation).stream())
                .map(MoreElements::asType)
                .collect(toList());

        final List<TypeElement> viewTypes = annotations
                .stream()
                .filter(annotation -> isAnnotationOfType(annotation, MaterializedView.class))
                .flatMap(annotation -> roundEnv.getElementsAnnotatedWith(annotation).stream())
                .map(MoreElements::asType)
                .collect(toList());

        final List<TypeElement> types = CollectionsHelper.appendAll(tableTypes, viewTypes);

        validateEntityNames(types);

        final List<EntityMetaSignature> tableSignatures = tableTypes
                .stream()
                .map(x -> entityParser.parseEntity(x, parsingContext))
                .collect(toList());

        final List<EntityMetaSignature> viewSignatures = viewTypes
                .stream()
                .map(x -> entityParser.parseView(x, parsingContext))
                .collect(toList());

        final List<EntityMetaSignature> tableAndViewSignatures = CollectionsHelper.appendAll(tableSignatures, viewSignatures);

        validateViewsAgainstBaseTable(aptUtils, viewSignatures, tableSignatures);
        return tableAndViewSignatures;
    }

    private GlobalParsingContext parseCodecRegistry(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        final GlobalParsingContext parsingContext = new GlobalParsingContext();
        parseCodecRegistry(parsingContext, annotations, roundEnv);
        return parsingContext;
    }

    private void validateEntityNames(List<TypeElement> entityTypes) {
        Map<String, String> entities = new HashedMap();
        for (TypeElement entityType : entityTypes) {
            final String className = entityType.getSimpleName().toString();
            final String FQCN = entityType.getQualifiedName().toString();
            if (entities.containsKey(className)) {
                final String existingFQCN = entities.get(className);
                aptUtils.printError("%s and %s both share the same class name, it is forbidden by Achilles",
                        FQCN, existingFQCN);
                throw new IllegalStateException(format("%s and %s both share the same class name, it is forbidden by Achilles",
                        FQCN, existingFQCN));
            } else {
                entities.put(className, FQCN);
            }
        }
    }

    private void parseCodecRegistry(GlobalParsingContext parsingContext, Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        final boolean hasCompileTimeCodecRegistry = annotations
                .stream()
                .filter(annotation -> isAnnotationOfType(annotation, CodecRegistry.class))
                .findFirst().isPresent();
        if (hasCompileTimeCodecRegistry) {
            aptUtils.printNote("[Achilles] Parsing compile-time codec registry");
            new CodecRegistryParser(aptUtils).parseCodecs(roundEnv, parsingContext);
        }
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Sets.newHashSet(Table.class.getCanonicalName(),
                MaterializedView.class.getCanonicalName(),
                CodecRegistry.class.getCanonicalName(),
                FunctionRegistry.class.getCanonicalName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

}
