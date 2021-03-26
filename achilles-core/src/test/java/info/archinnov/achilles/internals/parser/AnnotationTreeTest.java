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

package info.archinnov.achilles.internals.parser;

import static com.google.auto.common.MoreTypes.isTypeOf;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;

import org.junit.Ignore;
import org.junit.Test;
import org.truth0.Truth;

import com.google.testing.compile.JavaSourceSubjectFactory;
import com.squareup.javapoet.ClassName;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.parser.context.CodecContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.codecs.IntToStringCodec;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForAnnotationTree;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.Tuple3;

public class AnnotationTreeTest extends AbstractTestProcessor {

    private GlobalParsingContext globalParsingContext = GlobalParsingContext.defaultContext();
    @Test
    public void should_build_annotation_tree_for_map_javac() throws Exception {

        /**
         *     private Map<@JSON @Frozen Integer,
         *                  Map<@Frozen Integer,
         *                                      Tuple3<String,
         *                                              @Frozen @EmptyCollectionIfNull Integer,
         *                                              @Enumerated(value = Enumerated.Encoding.NAME) Date>>> map;
         */
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement mapElt = findFieldByName(typeElement, "map");

                // Map<@JSON @Frozen Integer, ...
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, mapElt);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());

                assertThat(annotationTree).isNotNull();
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(EmptyCollectionIfNull.class.getSimpleName(), Frozen.class.getSimpleName());

                // @JSON @Frozen Integer,
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(JSON.class.getSimpleName(), Frozen.class.getSimpleName());

                // Map<@Frozen Integer, Tuple3 ...
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @Frozen Integer
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Frozen.class.getSimpleName());

                // Tuple3<String, ...
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Tuple3.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // String
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(String.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @Frozen @EmptyCollectionIfNull Integer
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Frozen.class.getSimpleName(), EmptyCollectionIfNull.class.getSimpleName());

                // @Enumerated(value = Enumerated.Encoding.NAME) Date
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Date.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Enumerated.class.getSimpleName());

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_build_annotation_tree_for_map_ecj() throws Exception {

        /**
         *     private Map<@JSON @Frozen Integer,
         *                  Map<@Frozen Integer,
         *                                      Tuple3<String,
         *                                              @Frozen @EmptyCollectionIfNull Integer,
         *                                              @Enumerated(value = Enumerated.Encoding.NAME) Date>>> map;
         */


        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement mapElt = findFieldByName(typeElement, "map");

                // Map<@JSON @Frozen Integer, ...
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, mapElt);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());

                assertThat(annotationTree).isNotNull();
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(EmptyCollectionIfNull.class.getSimpleName(), Frozen.class.getSimpleName());

                // @JSON @Frozen Integer,
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(JSON.class.getSimpleName(), Frozen.class.getSimpleName());

                // Map<@Frozen Integer, Tuple3 ...
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @Frozen Integer
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Frozen.class.getSimpleName());

                // Tuple3<String, ...
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Tuple3.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // String
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(String.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @Frozen @EmptyCollectionIfNull Integer
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Frozen.class.getSimpleName(), EmptyCollectionIfNull.class.getSimpleName());

                // @Enumerated(value = Enumerated.Encoding.NAME) Date
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Date.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Enumerated.class.getSimpleName());

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_trees_for_other_fields_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement idElm = findFieldByName(typeElement, "id");
                final VariableElement timeElm = findFieldByName(typeElement, "time");
                final VariableElement listElm = findFieldByName(typeElement, "list");
                final VariableElement setElm = findFieldByName(typeElement, "set");

                //   @Enumerated(value = Enumerated.Encoding.NAME) private Long id;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, idElm);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Long.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Enumerated.class.getSimpleName());

                // private @JSON Date time;
                annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, timeElm);
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Date.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(JSON.class.getSimpleName());

                // private List<Integer> list;
                annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, listElm);
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(List.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // private Set<@Enumerated(value = Enumerated.Encoding.NAME) Double> set;
                annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, setElm);
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Set.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @Enumerated(value = Enumerated.Encoding.NAME) Double
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Double.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Enumerated.class.getSimpleName());


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_build_trees_for_other_fields_javac_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement idElm = findFieldByName(typeElement, "id");
                final VariableElement timeElm = findFieldByName(typeElement, "time");
                final VariableElement listElm = findFieldByName(typeElement, "list");
                final VariableElement setElm = findFieldByName(typeElement, "set");

                //   @Enumerated(value = Enumerated.Encoding.NAME) private Long id;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, idElm);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Long.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Enumerated.class.getSimpleName());

                // private @JSON Date time;
                annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, timeElm);
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Date.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(JSON.class.getSimpleName());

                // private List<Integer> list;
                annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, listElm);
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(List.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // private Set<@Enumerated(value = Enumerated.Encoding.NAME) Double> set;
                annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, setElm);
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Set.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @Enumerated(value = Enumerated.Encoding.NAME) Double
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Double.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Enumerated.class.getSimpleName());


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_trees_json_map_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement jsonMap = findFieldByName(typeElement, "jsonMap");

                // @JSON private Map<@JSON Integer, List<Integer>> jsonMap;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, jsonMap);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(JSON.class.getSimpleName());

                assertThat(annotationTree.hasNext()).isFalse();


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_build_trees_json_map_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement jsonMap = findFieldByName(typeElement, "jsonMap");

                // @JSON private Map<@JSON Integer, List<Integer>> jsonMap;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, jsonMap);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(JSON.class.getSimpleName());

                assertThat(annotationTree.hasNext()).isFalse();


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_trees_map_with_nested_json_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement mapWithNestedJson = findFieldByName(typeElement, "mapWithNestedJson");

                // private Map<Integer, @JSON List<Map<Integer, String>>> mapWithNestedJson;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, mapWithNestedJson);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // Integer
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @JSON List<Map<Integer, String>>
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(List.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(JSON.class.getSimpleName());

                assertThat(annotationTree.hasNext()).isFalse();


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_build_trees_map_with_nested_json_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement mapWithNestedJson = findFieldByName(typeElement, "mapWithNestedJson");

                // private Map<Integer, @JSON List<Map<Integer, String>>> mapWithNestedJson;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, mapWithNestedJson);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // Integer
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @JSON List<Map<Integer, String>>
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(List.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(JSON.class.getSimpleName());

                assertThat(annotationTree.hasNext()).isFalse();


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_trees_map_for_level1_nesting_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement level1NestingElm = findFieldByName(typeElement, "level1Nesting");

                // private List<Map<Integer,String>> level1Nesting;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, level1NestingElm);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(List.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // Map<Integer,String>
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // Integer
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // String
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(String.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_build_trees_map_for_level1_nesting_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement level1NestingElm = findFieldByName(typeElement, "level1Nesting");

                // private List<Map<Integer,String>> level1Nesting;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, level1NestingElm);
                Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(List.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // Map<Integer,String>
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // Integer
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // String
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(String.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_treemap_for_codec_annotation_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement mapWithCodec = findFieldByName(typeElement, "mapWithCodec");

                // private Map<@Codec(IntToStringCodec.class) Integer, String> mapWithCodec;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, mapWithCodec).next();
                final Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Codec.class.getSimpleName());

                final CodecContext codecContext = annotationTree.getAnnotations().get(Codec.class).getTyped("codecContext");
                assertThat(codecContext.codecType).isEqualTo(ClassName.get(IntToStringCodec.class));
                assertThat(codecContext.sourceType).isEqualTo(ClassName.get(Integer.class));
                assertThat(codecContext.targetType).isEqualTo(ClassName.get(String.class));

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_build_treemap_for_codec_annotation_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement mapWithCodec = findFieldByName(typeElement, "mapWithCodec");

                // private Map<@Codec(IntToStringCodec.class) Integer, String> mapWithCodec;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, mapWithCodec).next();
                final Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Codec.class.getSimpleName());

                final CodecContext codecContext = annotationTree.getAnnotations().get(Codec.class).getTyped("codecContext");
                assertThat(codecContext.codecType).isEqualTo(ClassName.get(IntToStringCodec.class));
                assertThat(codecContext.sourceType).isEqualTo(ClassName.get(Integer.class));
                assertThat(codecContext.targetType).isEqualTo(ClassName.get(String.class));

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_treemap_for_computed_annotation_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement writetime = findFieldByName(typeElement, "writetime");

                // @Computed(function = "writetime", alias = "writetime_col", cqlClass = Long.class, targetColumns = {"id", "value"})
                // private Long writetime;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, writetime);
                final Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Long.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Computed.class.getSimpleName());

                final TypedMap typedMap = annotationTree.getAnnotations().get(Computed.class);
                assertThat(typedMap.<String>getTyped("function")).isEqualTo("writetime");
                assertThat(typedMap.<String>getTyped("alias")).isEqualTo("writetime_col");
                assertThat(typedMap.<Class<?>>getTyped("cqlClass")).isEqualTo(Long.class);
                assertThat(typedMap.<List<String>>getTyped("targetColumns")).containsExactly("id", "value");

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_build_treemap_for_computed_annotation_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement writetime = findFieldByName(typeElement, "writetime");

                // @Computed(function = "writetime", alias = "writetime_col", cqlClass = Long.class, targetColumns = {"id", "value"})
                // private Long writetime;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, writetime);
                final Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Long.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Computed.class.getSimpleName());

                final TypedMap typedMap = annotationTree.getAnnotations().get(Computed.class);
                assertThat(typedMap.<String>getTyped("function")).isEqualTo("writetime");
                assertThat(typedMap.<String>getTyped("alias")).isEqualTo("writetime_col");
                assertThat(typedMap.<Class<?>>getTyped("cqlClass")).isEqualTo(Long.class);
                assertThat(typedMap.<List<String>>getTyped("targetColumns")).containsExactly("id", "value");

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_treemap_for_clustering_column_annotation_java() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement clusteringCol = findFieldByName(typeElement, "clusteringCol");

                // @ClusteringColumn(value = 2, asc = false)
                // private UUID clusteringCol;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, clusteringCol);
                final Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(UUID.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(ClusteringColumn.class.getSimpleName());

                final TypedMap typedMap = annotationTree.getAnnotations().get(ClusteringColumn.class);
                assertThat(typedMap.<Integer>getTyped("order")).isEqualTo(2);
                assertThat(typedMap.<Boolean>getTyped("asc")).isFalse();

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_build_treemap_for_clustering_column_annotation_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement clusteringCol = findFieldByName(typeElement, "clusteringCol");

                // @ClusteringColumn(value = 2, asc = false)
                // private UUID clusteringCol;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, clusteringCol);
                final Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(UUID.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(ClusteringColumn.class.getSimpleName());

                final TypedMap typedMap = annotationTree.getAnnotations().get(ClusteringColumn.class);
                assertThat(typedMap.<Integer>getTyped("order")).isEqualTo(2);
                assertThat(typedMap.<Boolean>getTyped("asc")).isFalse();

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_build_treemap_for_frozen_udt_annotation_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement testUdt = findFieldByName(typeElement, "testUdt");

                // @Frozen
                // private TestUDT testUdt;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, testUdt);
                final Set<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(TestUDT.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsOnly(Frozen.class.getSimpleName());

                final TypedMap typedMap = annotationTree.getAnnotations().get(Frozen.class);
                assertThat(typedMap.isEmpty()).isTrue();

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    private Set<String> getAnnotationNames(Map<Class<? extends Annotation>, TypedMap> annotationInfo) {
        return annotationInfo
                .keySet()
                .stream()
                .map(x -> x.getSimpleName())
                .collect(Collectors.toSet());
    }

    private VariableElement findFieldByName(TypeElement typeElement, String fieldName) {
        return ElementFilter.fieldsIn(typeElement.getEnclosedElements())
                .stream().filter(x -> x.getSimpleName().contentEquals(fieldName))
                .findFirst()
                .get();
    }


}