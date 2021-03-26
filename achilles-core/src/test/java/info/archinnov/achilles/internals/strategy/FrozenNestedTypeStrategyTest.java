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

package info.archinnov.achilles.internals.strategy;

import static info.archinnov.achilles.internals.apt.AptUtils.findFieldInType;

import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

import org.junit.Test;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.parser.AnnotationTree;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.parser.validator.cassandra2_1.NestedTypeValidator2_1;
import info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes;

public class FrozenNestedTypeStrategyTest extends AbstractTestProcessor {

    GlobalParsingContext globalParsingContext = GlobalParsingContext.defaultContext();

    @Test
    public void should_fail_for_non_frozen_udt() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private TestUDT testUDT;
            VariableElement elm = findFieldInType(typeElement, "testUDT");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "testUDT", rawClass);
        });
        failTestWithMessage("UDT class " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT' " +
                "in field 'testUDT' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_udtValue() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private UDTValue udtValue;
            VariableElement elm = findFieldInType(typeElement, "udtValue");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "udtValue", rawClass);
        });
        failTestWithMessage("UDTValue " +
                "in field 'udtValue' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_tupleValue() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private TupleValue tupleValue;
            VariableElement elm = findFieldInType(typeElement, "tupleValue");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "tupleValue", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_list_list() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private List<List<String>> listList;
            VariableElement elm = findFieldInType(typeElement, "listList");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "listList", rawClass);
        });
        failTestWithMessage("collections/array type/UDT " +
                "'java.util.List<java.lang.String>' " +
                "in 'listList' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_list_udt() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private List<TestUDT> listUdt;
            VariableElement elm = findFieldInType(typeElement, "listUdt");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "listUdt", rawClass);
        });
        failTestWithMessage("collections/array type/UDT " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT' " +
                "in 'listUdt' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_list_tuple() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private List<Tuple3<Integer, String, String>> listTuple;
            VariableElement elm = findFieldInType(typeElement, "listTuple");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "listTuple", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_list_tupleValue() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private List<TupleValue> listTupleValue;
            VariableElement elm = findFieldInType(typeElement, "listTupleValue");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "listTupleValue", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_list_udtValue() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private List<UDTValue> listUdtValue;
            VariableElement elm = findFieldInType(typeElement, "listUdtValue");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "listUdtValue", rawClass);
        });
        failTestWithMessage("collections/array type/UDT " +
                "'com.datastax.driver.core.UDTValue' " +
                "in 'listUdtValue' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_map_list() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<Integer, List<String>> mapList;
            VariableElement elm = findFieldInType(typeElement, "mapList");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapList", rawClass);
        });
        failTestWithMessage("collections/array type/UDT " +
                "'java.util.List<java.lang.String>' " +
                "in 'mapList' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_map_udt() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<Integer, TestUDT> mapUdt;
            VariableElement elm = findFieldInType(typeElement, "mapUdt");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapUdt", rawClass);
        });
        failTestWithMessage("collections/array type/UDT " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT' " +
                "in 'mapUdt' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_map_tuple() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<Integer, Tuple2<Integer, String>> mapTuple;
            VariableElement elm = findFieldInType(typeElement, "mapTuple");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapTuple", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_map_tupleValue() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<Integer, TupleValue> mapTupleValue;
            VariableElement elm = findFieldInType(typeElement, "mapTupleValue");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapTupleValue", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_map_udtValue() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<Integer, UDTValue> mapUDTValue;
            VariableElement elm = findFieldInType(typeElement, "mapUDTValue");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapUDTValue", rawClass);
        });
        failTestWithMessage("collections/array type/UDT " +
                "'com.datastax.driver.core.UDTValue' " +
                "in 'mapUDTValue' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_map_listKey() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<List<Integer>, String> mapListKey;
            VariableElement elm = findFieldInType(typeElement, "mapListKey");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapListKey", rawClass);
        });
        failTestWithMessage("Map key of type collection/UDT " +
                "'java.util.List<java.lang.Integer>' " +
                "in 'mapListKey' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_map_udtKey() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<TestUDT, String> mapUdtKey;
            VariableElement elm = findFieldInType(typeElement, "mapUdtKey");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapUdtKey", rawClass);
        });
        failTestWithMessage("Map key of type collection/UDT " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT' " +
                "in 'mapUdtKey' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_map_tupleKey() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<Tuple4<Integer, Integer, String, String>, String> mapTupleKey;
            VariableElement elm = findFieldInType(typeElement, "mapTupleKey");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapTupleKey", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_map_tupleValueKey() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<TupleValue, String> mapTupleValueKey;
            VariableElement elm = findFieldInType(typeElement, "mapTupleValueKey");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapTupleValueKey", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_for_non_frozen_map_udtValueKey() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<UDTValue, String> mapUDTValueKey;
            VariableElement elm = findFieldInType(typeElement, "mapUDTValueKey");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "mapUDTValueKey", rawClass);
        });
        failTestWithMessage("Map key of type collection/UDT " +
                "'com.datastax.driver.core.UDTValue' " +
                "in 'mapUDTValueKey' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes' " +
                "should be annotated with @Frozen", TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_tuple_list() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Tuple5<Integer, List<String>, Integer, Integer, String> tupleList;
            VariableElement elm = findFieldInType(typeElement, "tupleList");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "tupleList", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_tuple_udt() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Tuple6<Integer, Integer, Integer, Integer, String, TestUDT> tupleUDT;
            VariableElement elm = findFieldInType(typeElement, "tupleUDT");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "tupleUDT", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_tuple_udtValue() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Tuple7<Integer, Integer, Integer, Integer, String, String, UDTValue> tupleUDTValue;
            VariableElement elm = findFieldInType(typeElement, "tupleUDTValue");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "tupleUDTValue", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_tuple_tupleValue() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Tuple8<Integer, Integer, Integer, Integer, String, String, String, TupleValue> tupleTupleValue;
            VariableElement elm = findFieldInType(typeElement, "tupleTupleValue");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "tupleTupleValue", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_not_fail_for_non_frozen_tuple_map() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Tuple9<Integer, Integer, Integer, Integer, String, String, String, String, Map<Integer, String>> tupleMap;
            VariableElement elm = findFieldInType(typeElement, "tupleMap");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "tupleMap", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_validate_index_depth_1() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // @Index private String indexedString;
            VariableElement elm = findFieldInType(typeElement, "indexedString");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "indexedString", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_validate_index_depth_2() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<Integer, @Frozen @Index List<String>> indexedMapList;
            VariableElement elm = findFieldInType(typeElement, "indexedMapList");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "indexedMapList", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_validate_index_depth_2_as_map_key() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<@Index Integer, @Frozen List<String>> indexedMapKey;
            VariableElement elm = findFieldInType(typeElement, "indexedMapKey");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "indexedMapKey", rawClass);
        });
        launchTest(TestEntityWithNestedTypes.class);
    }

    @Test
    public void should_fail_validating_index_depth_gt_2() throws Exception {
        setExec(aptUtils -> {
            final NestedTypeValidator2_1 strategy = new NestedTypeValidator2_1();
            final String className = TestEntityWithNestedTypes.class.getCanonicalName();
            final TypeName rawClass = ClassName.get(TestEntityWithNestedTypes.class);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            // private Map<Integer, @Frozen Map<Integer, @Index List<String>>> nestedIndex;
            VariableElement elm = findFieldInType(typeElement, "nestedIndex");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            strategy.validate(aptUtils, annotationTree, "nestedIndex", rawClass);
        });
        failTestWithMessage("@Index annotation cannot be nested for depth > 2 for field 'nestedIndex' of class 'info.archinnov.achilles.internals.sample_classes.parser.strategy.TestEntityWithNestedTypes'",
                TestEntityWithNestedTypes.class);
    }
}