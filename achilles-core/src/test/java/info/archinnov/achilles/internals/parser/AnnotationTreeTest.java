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

package info.archinnov.achilles.internals.parser;

import static com.google.auto.common.MoreTypes.isTypeOf;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.truth0.Truth;

import com.google.testing.compile.JavaSourceSubjectFactory;

import info.archinnov.achilles.annotations.EmptyCollectionIfNull;
import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.JSON;
import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.apt_utils.AptAssertOK;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForAnnotationTree;
import info.archinnov.achilles.type.tuples.Tuple3;

@RunWith(MockitoJUnitRunner.class)
public class AnnotationTreeTest extends AbstractTestProcessor {

    @Test
    public void should_build_annotation_tree_for_map() throws Exception {

        /**
         *     private Map<@JSON @Frozen Integer,
         *                  Map<@Frozen Integer,
         *                                      Tuple3<String,
         *                                              @Frozen @EmptyCollectionIfNull Integer,
         *                                              @Enumerated(value = Enumerated.Encoding.NAME) Date>>> map;
         */


        AptAssertOK exec = aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement mapElt = findFieldByName(typeElement, "map");

                // Map<@JSON @Frozen Integer, ...
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, mapElt);
                List<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());

                assertThat(annotationTree).isNotNull();
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsExactly(EmptyCollectionIfNull.class.getSimpleName(), Frozen.class.getSimpleName());

                // @JSON @Frozen Integer,
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsExactly(JSON.class.getSimpleName(), Frozen.class.getSimpleName());

                // Map<@Frozen Integer, Tuple3 ...
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @Frozen Integer
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Integer.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsExactly(Frozen.class.getSimpleName());

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
                assertThat(annotationNames).containsExactly(Frozen.class.getSimpleName(), EmptyCollectionIfNull.class.getSimpleName());

                // @Enumerated(value = Enumerated.Encoding.NAME) Date
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Date.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsExactly(Enumerated.class.getSimpleName());

            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        };

        setExec(exec);

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }


    @Test
    public void should_build_trees_for_other_fields() throws Exception {
        //Given
        AptAssertOK exec = aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement idElm = findFieldByName(typeElement, "id");
                final VariableElement timeElm = findFieldByName(typeElement, "time");
                final VariableElement listElm = findFieldByName(typeElement, "list");
                final VariableElement setElm = findFieldByName(typeElement, "set");

                //   @Enumerated(value = Enumerated.Encoding.NAME) private Long id;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, idElm);
                List<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Long.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsExactly(Enumerated.class.getSimpleName());

                // private @JSON Date time;
                annotationTree = AnnotationTree.buildFrom(aptUtils, timeElm);
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Date.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsExactly(JSON.class.getSimpleName());

                // private List<Integer> list;
                annotationTree = AnnotationTree.buildFrom(aptUtils, listElm);
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(List.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // private Set<@Enumerated(value = Enumerated.Encoding.NAME) Double> set;
                annotationTree = AnnotationTree.buildFrom(aptUtils, setElm);
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Set.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).isEmpty();

                // @Enumerated(value = Enumerated.Encoding.NAME) Double
                annotationTree = annotationTree.next();
                annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Double.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsExactly(Enumerated.class.getSimpleName());


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        };

        setExec(exec);

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_trees_json_map() throws Exception {
        //Given
        AptAssertOK exec = aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement jsonMap = findFieldByName(typeElement, "jsonMap");

                // @JSON private Map<@JSON Integer, List<Integer>> jsonMap;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, jsonMap);
                List<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
                assertThat(isTypeOf(Map.class, annotationTree.getCurrentType())).isTrue();
                assertThat(annotationNames).containsExactly(JSON.class.getSimpleName());

                assertThat(annotationTree.hasNext()).isFalse();


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        };

        setExec(exec);

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_trees_map_with_nested_json() throws Exception {
        //Given
        AptAssertOK exec = aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement mapWithNestedJson = findFieldByName(typeElement, "mapWithNestedJson");

                // private Map<Integer, @JSON List<Map<Integer, String>>> mapWithNestedJson;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, mapWithNestedJson);
                List<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
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
                assertThat(annotationNames).containsExactly(JSON.class.getSimpleName());

                assertThat(annotationTree.hasNext()).isFalse();


            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        };

        setExec(exec);

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_build_trees_map_for_level1_nesting() throws Exception {
        //Given
        AptAssertOK exec = aptUtils -> {
            try {
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForAnnotationTree.class.getCanonicalName());
                final VariableElement level1NestingElm = findFieldByName(typeElement, "level1Nesting");

                // private List<Map<Integer,String>> level1Nesting;
                AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, level1NestingElm);
                List<String> annotationNames = getAnnotationNames(annotationTree.getAnnotations());
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
        };

        setExec(exec);

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    private List<String> getAnnotationNames(List<? extends AnnotationMirror> annotationMirrors) {
        return annotationMirrors
                .stream()
                .map(x -> x.getAnnotationType().asElement().getSimpleName().toString())
                .collect(Collectors.toList());
    }

    private VariableElement findFieldByName(TypeElement typeElement, String fieldName) {
        return ElementFilter.fieldsIn(typeElement.getEnclosedElements())
                .stream().filter(x -> x.getSimpleName().contentEquals(fieldName))
                .findFirst()
                .get();
    }


}