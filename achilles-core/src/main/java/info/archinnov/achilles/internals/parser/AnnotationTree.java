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

import static info.archinnov.achilles.internals.apt.AptUtils.*;
import static info.archinnov.achilles.internals.parser.validator.FieldValidator.validateCompatibleCodecAnnotationsOnField;
import static java.util.stream.Collectors.toList;

import java.util.*;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Name;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import com.google.auto.common.MoreTypes;
import com.sun.tools.javac.code.Attribute;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.code.SymbolMetadata;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.type.tuples.*;

public class AnnotationTree {

    private List<AnnotationMirror> annotations = new ArrayList<>();
    private TypeMirror currentType;
    private AnnotationTree next;
    private int depth = 1;

    AnnotationTree(TypeMirror currentType, List<AnnotationMirror> annotations, int currentDepth) {
        this.annotations = annotations;
        this.currentType = currentType;
        this.depth = currentDepth;
    }

    public static AnnotationTree buildFrom(AptUtils aptUtils, VariableElement varElm) {
        final String fieldName = varElm.getSimpleName().toString();
        final Name className = enclosingClass(varElm).getQualifiedName();
        final TypeMirror currentType = varElm.asType();

        final Frozen frozen = varElm.getAnnotation(Frozen.class);
        final JSON json = varElm.getAnnotation(JSON.class);
        final Enumerated enumerated = varElm.getAnnotation(Enumerated.class);
        final Codec codec = varElm.getAnnotation(Codec.class);
        final Computed computed = varElm.getAnnotation(Computed.class);
        final Counter counter = varElm.getAnnotation(Counter.class);
        final TimeUUID timeUUID = varElm.getAnnotation(TimeUUID.class);

        validateCompatibleCodecAnnotationsOnField(aptUtils, fieldName, className, frozen, json, enumerated, codec, computed, counter, timeUUID);

        final List<? extends TypeMirror> nestedTypes = currentType.getKind() == TypeKind.DECLARED ?
                MoreTypes.asDeclared(currentType).getTypeArguments() : Arrays.asList();
        final SymbolMetadata metadata = ((Symbol.VarSymbol) varElm).getMetadata();

        final List<Attribute.TypeCompound> typeAttributes = metadata == null ?
                Arrays.asList() : metadata.getTypeAttributes();

        final List<AnnotationMirror> annotationMirrors = varElm.getAnnotationMirrors()
                .stream()
                .filter(x ->
                                areSameByClass(x, JSON.class) ||
                                        areSameByClass(x, EmptyCollectionIfNull.class) ||
                                        areSameByClass(x, Enumerated.class) ||
                                        areSameByClass(x, Frozen.class) ||
                                        areSameByClass(x, Computed.class) ||
                                        areSameByClass(x, Counter.class) ||
                                        areSameByClass(x, TimeUUID.class) ||
                                        areSameByClass(x, Codec.class)
                )
                .map(x -> (AnnotationMirror)x)
                .collect(toList());

        final AnnotationTree annotationTree = new AnnotationTree(currentType, annotationMirrors, 1);
        buildTree(aptUtils, annotationTree, 1, nestedTypes, typeAttributes);

        return annotationTree;
    }

    private static AnnotationTree buildTree(AptUtils aptUtils, AnnotationTree annotationTree, int depth,
                                            List<? extends TypeMirror> nestedTypes, List<Attribute.TypeCompound> typeAttributes) {

        final TypeMirror currentType = annotationTree.currentType;

        final boolean hasJson = containsAnnotation(annotationTree.getAnnotations(), JSON.class);

        if (hasJson) {
            return annotationTree;
        }

        if (isPrimitive(currentType) || isArray(currentType) || isAnEnum(currentType)) {
            return annotationTree;
        } else if (aptUtils.isAssignableFrom(Tuple1.class, currentType) ||
                aptUtils.isAssignableFrom(List.class, currentType) ||
                aptUtils.isAssignableFrom(Set.class, currentType)) {

            final TypeMirror typeMirror = nestedTypes.get(0);
            final List<AnnotationMirror> annotations = typeAttributes
                    .stream()
                    .filter(x -> x.getPosition().location.size() == depth)
                    .collect(toList());
            final AnnotationTree newTree = annotationTree.addNext(new AnnotationTree(typeMirror, annotations, depth + 1));
            final ArrayList<Attribute.TypeCompound> newTypeAttributes = new ArrayList<>(typeAttributes);
            newTypeAttributes.removeAll(annotations);

            return buildTree(aptUtils, newTree, depth + 1, getTypeArguments(typeMirror), newTypeAttributes);

        } else if (aptUtils.isAssignableFrom(Tuple2.class, currentType) || aptUtils.isAssignableFrom(Map.class, annotationTree.currentType)) {
            return buildTreeForTuple(aptUtils, annotationTree, depth, 2, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple3.class, currentType)) {
            return buildTreeForTuple(aptUtils, annotationTree, depth, 3, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple4.class, currentType)) {
            return buildTreeForTuple(aptUtils, annotationTree, depth, 4, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple5.class, currentType)) {
            return buildTreeForTuple(aptUtils, annotationTree, depth, 5, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple6.class, currentType)) {
            return buildTreeForTuple(aptUtils, annotationTree, depth, 6, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple7.class, currentType)) {
            return buildTreeForTuple(aptUtils, annotationTree, depth, 7, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple8.class, currentType)) {
            return buildTreeForTuple(aptUtils, annotationTree, depth, 8, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple9.class, currentType)) {
            return buildTreeForTuple(aptUtils, annotationTree, depth, 9, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple10.class, currentType)) {
            return buildTreeForTuple(aptUtils, annotationTree, depth, 10, nestedTypes, typeAttributes);
        } else if (MoreTypes.asTypeElement(currentType).getAnnotation(UDT.class) != null) {
            return annotationTree;
        } else if (nestedTypes.size() == 0) {
            return annotationTree;
        } else {
            throw new IllegalStateException("Unknown current type : " + currentType.toString());
        }
    }

    private static List<? extends TypeMirror> getTypeArguments(TypeMirror typeMirror) {
        return MoreTypes.asDeclared(typeMirror).getTypeArguments();
    }

    private static AnnotationTree buildTreeForTuple(AptUtils aptUtils, AnnotationTree annotationTree,
                                                    int depth, int cardinality, List<? extends TypeMirror> nestedTypes, List<Attribute.TypeCompound> typeAttributes) {

        final List<Attribute.TypeCompound> annotations = typeAttributes
                .stream()
                .filter(x -> x.getPosition().location.size() == depth)
                .collect(toList());
        final ArrayList<Attribute.TypeCompound> newTypeAttributes = new ArrayList<>(typeAttributes);
        newTypeAttributes.removeAll(annotations);

        AnnotationTree newTreeN;
        AnnotationTree recursiveTreeN = annotationTree;

        for (int i = 0; i < cardinality; i++) {
            final TypeMirror typeMirrorN = nestedTypes.get(i);
            final int j = i;
            final List<AnnotationMirror> annotsN = annotations
                    .stream()
                    .filter(x -> x.getPosition().location.get(depth - 1).arg == j)
                    .collect(toList());
            newTreeN = recursiveTreeN.addNext(new AnnotationTree(typeMirrorN, annotsN, depth + 1));
            recursiveTreeN = buildTree(aptUtils, newTreeN, depth + 1, getTypeArguments(typeMirrorN), newTypeAttributes);
        }
        return recursiveTreeN;
    }

    AnnotationTree addNext(AnnotationTree next) {
        this.next = next;
        return this.next;
    }

    public boolean hasNext() {
        return this.next != null;
    }

    public AnnotationTree next() {
        if (hasNext()) {
            return next;
        } else {
            throw new IllegalStateException("No more leaf for annotation tree");
        }
    }

    public List<AnnotationMirror> getAnnotations() {
        return annotations;
    }

    public TypeMirror getCurrentType() {
        return currentType;
    }

    public int depth() {
        return this.depth;
    }

}
