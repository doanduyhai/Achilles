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
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.*;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

import org.apache.commons.io.FileUtils;

import com.google.auto.common.MoreElements;
import com.google.auto.service.AutoService;
import com.google.common.collect.Sets;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.annotations.CodecRegistry;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.ManagerFactoryBuilderCodeGen;
import info.archinnov.achilles.internals.codegen.ManagerFactoryCodeGen;
import info.archinnov.achilles.internals.codegen.ManagerFactoryCodeGen.ManagersAndDSLClasses;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.parser.CodecRegistryParser;
import info.archinnov.achilles.internals.parser.CodecFactory;
import info.archinnov.achilles.internals.parser.EntityParser;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;


@AutoService(Processor.class)
public class AchillesProcessor extends AbstractProcessor {

    protected AptUtils aptUtils;
    protected EntityParser entityParser;

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
        if (!annotations.isEmpty() && !roundEnv.processingOver()) {

            final boolean hasCompileTimeCodecRegistry = annotations
                    .stream()
                    .filter(annotation -> isAnnotationOfType(annotation, CodecRegistry.class))
                    .findFirst().isPresent();

            Map<TypeName, CodecFactory.CodecInfo> codecRegistry = new HashMap<>();

            if (hasCompileTimeCodecRegistry) {
                aptUtils.printNote("[Achilles] Parsing compile-time codec registry");
                codecRegistry.putAll(new CodecRegistryParser(aptUtils).parseCodecs(roundEnv));
            }

            final GlobalParsingContext parsingContext = new GlobalParsingContext(codecRegistry);

            final List<EntityMetaSignature> entityMetas = annotations
                    .stream()
                    .filter(annotation -> isAnnotationOfType(annotation, Entity.class))
                    .flatMap(annotation -> roundEnv.getElementsAnnotatedWith(annotation).stream())
                    .map(MoreElements::asType)
                    .map(x -> entityParser.parseEntity(x, parsingContext))
                    .collect(toList());

            final TypeSpec managerFactoryBuilder = ManagerFactoryBuilderCodeGen.buildInstance();
            final ManagersAndDSLClasses managersAndDSLClasses = ManagerFactoryCodeGen.buildInstance(aptUtils, entityMetas, parsingContext);

            try {
                final FileObject resource = aptUtils.filer.getResource(StandardLocation.SOURCE_OUTPUT, GENERATED_PACKAGE, MANAGER_FACTORY_BUILDER_CLASS);
                final File generatedSourceFolder = new File(resource.toUri().getRawPath().replaceAll("(.+/info/archinnov/achilles/generated/).+", "$1"));

                aptUtils.printNote("[Achilles] Cleaning previously generated source files");
                FileUtils.deleteDirectory(generatedSourceFolder);

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
                for (EntityMetaSignature signature : entityMetas) {
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
            } catch (IllegalStateException e) {
                aptUtils.printError("Error while parsing: %s", e.getMessage(), e);
                e.printStackTrace();
            } catch (IOException e) {
                aptUtils.printError("Fail generating source file : %s", e.getMessage(), e);
                e.printStackTrace();
            } catch (Throwable throwable) {
                aptUtils.printError("Fail generating source file : %s", throwable.getMessage(), throwable);
                throwable.printStackTrace();
            }

        }
        return roundEnv.processingOver();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Sets.newHashSet(Entity.class.getCanonicalName(),
                CodecRegistry.class.getCanonicalName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

}
