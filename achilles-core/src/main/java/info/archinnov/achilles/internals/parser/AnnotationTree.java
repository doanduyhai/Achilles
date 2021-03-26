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

import static info.archinnov.achilles.internals.apt.AptUtils.*;
import static java.util.stream.Collectors.toList;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;
import javax.lang.model.element.*;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import org.eclipse.jdt.internal.compiler.apt.model.DeclaredTypeImplContainer;
import org.eclipse.jdt.internal.compiler.apt.model.TypeElementImpl;
import org.eclipse.jdt.internal.compiler.apt.model.TypeMirrorImpl;
import org.eclipse.jdt.internal.compiler.apt.model.VariableElementImpl;
import org.eclipse.jdt.internal.compiler.impl.BooleanConstant;
import org.eclipse.jdt.internal.compiler.impl.IntConstant;
import org.eclipse.jdt.internal.compiler.impl.StringConstant;
import org.eclipse.jdt.internal.compiler.lookup.*;

import com.google.auto.common.MoreTypes;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import com.sun.tools.javac.code.Attribute;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.code.SymbolMetadata;
import com.sun.tools.javac.code.TargetType;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.annotations.SASI.Analyzer;
import info.archinnov.achilles.annotations.SASI.IndexMode;
import info.archinnov.achilles.annotations.SASI.Normalization;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.context.*;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.*;

public class AnnotationTree {

    private Map<Class<? extends Annotation>, TypedMap> annotations = new LinkedHashMap<>();
    private TypeMirror currentType;
    private AnnotationTree next;
    private int depth = 1;

    AnnotationTree(TypeMirror currentType, Map<Class<? extends Annotation>, TypedMap> annotations, int currentDepth) {
        this.annotations = annotations;
        this.currentType = currentType;
        this.depth = currentDepth;
    }


    public static AnnotationTree buildFromMethodForReturnType(AptUtils aptUtils, ExecutableElement method) {
        if (isJavaCompiler(method)) {
            final TypeMirror returnType = method.getReturnType();
            final SymbolMetadata metadata = ((Symbol.MethodSymbol) method).getMetadata();
            final List<Attribute.TypeCompound> typeAttributes = metadata == null ? Arrays.asList() : metadata.getTypeAttributes();
            final Map<Class<? extends Annotation>, TypedMap> annotationInfo = typeAttributes
                    .stream()
                    .filter(x -> x.getPosition().type == TargetType.METHOD_RETURN && x.getPosition().location.size() == 0)
                    .map(x -> (AnnotationMirror) x)
                    .collect(Collectors.toMap(x -> toAnnotation_Javac(aptUtils, x),
                            x -> inspectSupportedAnnotation_Javac(aptUtils, returnType, x)));

            final AnnotationTree annotationTree = new AnnotationTree(returnType, annotationInfo, 1);

            final List<? extends TypeMirror> nestedTypes = returnType.getKind() == TypeKind.DECLARED ?
                    MoreTypes.asDeclared(returnType).getTypeArguments() : Arrays.asList();

            final List<Attribute.TypeCompound> nestedTypeAttributes = typeAttributes
                    .stream()
                    .filter(x -> x.getPosition().type == TargetType.METHOD_RETURN && x.getPosition().location.size() > 0)
                    .collect(toList());

            buildTree_Javac(aptUtils, annotationTree, 1, nestedTypes, nestedTypeAttributes);

            return annotationTree;

        } else if (isEclipseCompiler(method)) {
            final TypeMirror returnType = method.getReturnType();
            final List<? extends TypeMirror> nestedTypes = returnType.getKind() == TypeKind.DECLARED ?
                    MoreTypes.asDeclared(returnType).getTypeArguments() : Arrays.asList();
            final TypeBinding binding = (TypeBinding) DeclaredTypeImplContainer.from((TypeMirrorImpl) returnType).getBinding();
            final List<AnnotationBinding> annotationBindings = Arrays.asList(binding.getTypeAnnotations());
            final Map<Class<? extends Annotation>, TypedMap> annotationInfo = annotationBindings
                    .stream()
                    .filter(annotBinding -> {
                        final String annotationName = annotBinding.getAnnotationType().debugName();
                        return JSON.class.getCanonicalName().equals(annotationName) ||
                                EmptyCollectionIfNull.class.getCanonicalName().equals(annotationName) ||
                                Enumerated.class.getCanonicalName().equals(annotationName) ||
                                Frozen.class.getCanonicalName().equals(annotationName) ||
                                Computed.class.getCanonicalName().equals(annotationName) ||
                                Counter.class.getCanonicalName().equals(annotationName) ||
                                TimeUUID.class.getCanonicalName().equals(annotationName) ||
                                ASCII.class.getCanonicalName().equals(annotationName) ||
                                Codec.class.getCanonicalName().equals(annotationName) ||
                                RuntimeCodec.class.getCanonicalName().equals(annotationName) ||
                                Index.class.getCanonicalName().equals(annotationName) ||
                                PartitionKey.class.getCanonicalName().equals(annotationName) ||
                                ClusteringColumn.class.getCanonicalName().equals(annotationName);
                    })
                    .map(x -> inspectSupportedAnnotation_Ecj(aptUtils, returnType, x))
                    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

            final AnnotationTree annotationTree = new AnnotationTree(returnType, annotationInfo, 1);
            List<TypeBinding> typeBindings = new ArrayList<>();
            if (binding instanceof ParameterizedTypeBinding) {
                typeBindings = Arrays.asList(((ParameterizedTypeBinding) binding).typeArguments());
            }
            buildTree_Ecj(aptUtils, annotationTree, 1, nestedTypes, typeBindings);
            return annotationTree;

        } else {
            aptUtils.printError("Unknown compiler, only standard Java compiler and Eclipse ECJ compiler are supported");
            return null;
        }
    }

    public static List<AnnotationTree> buildFromMethodForParam(AptUtils aptUtils, ExecutableElement method) {
        if (isJavaCompiler(method)) {
            final List<AnnotationTree> annotationTrees = new ArrayList<>(method.getParameters().size());
            final SymbolMetadata metadata = ((Symbol.MethodSymbol) method).getMetadata();
            final List<Attribute.TypeCompound> typeAttributes = metadata == null ? Arrays.asList() : metadata.getTypeAttributes();
            final List<? extends VariableElement> parameters = method.getParameters();
            for(int i=0; i< parameters.size(); i++) {
                final int finalI = i;
                final VariableElement parameter = parameters.get(i);
                final TypeMirror typeMirror = parameter.asType();
                final Map<Class<? extends Annotation>, TypedMap> annotationInfo = typeAttributes
                        .stream()
                        .filter(x -> x.getPosition().parameter_index == finalI && x.getPosition().location.size() == 0)
                        .map(x -> (AnnotationMirror) x)
                        .collect(Collectors.toMap(x -> toAnnotation_Javac(aptUtils, x),
                                x -> inspectSupportedAnnotation_Javac(aptUtils, typeMirror, x)));

                final AnnotationTree annotationTree = new AnnotationTree(typeMirror, annotationInfo, 1);

                final List<? extends TypeMirror> nestedTypes = typeMirror.getKind() == TypeKind.DECLARED ?
                        MoreTypes.asDeclared(typeMirror).getTypeArguments() : Arrays.asList();

                final List<Attribute.TypeCompound> nestedTypeAttributes = typeAttributes
                        .stream()
                        .filter(x -> x.getPosition().parameter_index == finalI && x.getPosition().location.size() > 0)
                        .collect(toList());

                buildTree_Javac(aptUtils, annotationTree, 1, nestedTypes, nestedTypeAttributes);
                annotationTrees.add(annotationTree);
            }

            return annotationTrees;

        } else if (isEclipseCompiler(method)) {
            final List<AnnotationTree> annotationTrees = new ArrayList<>(method.getParameters().size());
            for (VariableElement varElm : method.getParameters()) {
                final TypeMirror currentType = varElm.asType();
                final List<? extends TypeMirror> nestedTypes = currentType.getKind() == TypeKind.DECLARED ?
                        MoreTypes.asDeclared(currentType).getTypeArguments() : Arrays.asList();

                final TypeBinding binding = (TypeBinding) DeclaredTypeImplContainer.from((TypeMirrorImpl) currentType).getBinding();
                final List<AnnotationBinding> annotationBindings = Arrays.asList(binding.getTypeAnnotations());
                final Map<Class<? extends Annotation>, TypedMap> annotationInfo = annotationBindings
                        .stream()
                        .filter(annotBinding -> {
                            final String annotationName = annotBinding.getAnnotationType().debugName();
                            return JSON.class.getCanonicalName().equals(annotationName) ||
                                    EmptyCollectionIfNull.class.getCanonicalName().equals(annotationName) ||
                                    Enumerated.class.getCanonicalName().equals(annotationName) ||
                                    Frozen.class.getCanonicalName().equals(annotationName) ||
                                    Computed.class.getCanonicalName().equals(annotationName) ||
                                    Counter.class.getCanonicalName().equals(annotationName) ||
                                    TimeUUID.class.getCanonicalName().equals(annotationName) ||
                                    ASCII.class.getCanonicalName().equals(annotationName) ||
                                    Codec.class.getCanonicalName().equals(annotationName) ||
                                    RuntimeCodec.class.getCanonicalName().equals(annotationName) ||
                                    Index.class.getCanonicalName().equals(annotationName) ||
                                    PartitionKey.class.getCanonicalName().equals(annotationName) ||
                                    ClusteringColumn.class.getCanonicalName().equals(annotationName);
                        })
                        .map(x -> inspectSupportedAnnotation_Ecj(aptUtils, currentType, x))
                        .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

                final AnnotationTree annotationTree = new AnnotationTree(currentType, annotationInfo, 1);
                List<TypeBinding> typeBindings = new ArrayList<>();
                if (binding instanceof ParameterizedTypeBinding) {
                    typeBindings = Arrays.asList(((ParameterizedTypeBinding) binding).typeArguments());
                }

                buildTree_Ecj(aptUtils, annotationTree, 1, nestedTypes, typeBindings);
                annotationTrees.add(annotationTree);
            }

            return annotationTrees;

        } else {
            aptUtils.printError("Unknown compiler, only standard Java compiler and Eclipse ECJ compiler are supported");
            return null;
        }
    }

    public static AnnotationTree buildFrom(AptUtils aptUtils, GlobalParsingContext parsingContext, VariableElement varElm) {
        final String fieldName = varElm.getSimpleName().toString();
        final Name className = enclosingClass(varElm).getQualifiedName();
        final TypeMirror currentType = varElm.asType();

        final Frozen frozen = varElm.getAnnotation(Frozen.class);
        final JSON json = varElm.getAnnotation(JSON.class);
        final Enumerated enumerated = varElm.getAnnotation(Enumerated.class);
        final Codec codec = varElm.getAnnotation(Codec.class);
        final RuntimeCodec runtimeCodec = varElm.getAnnotation(RuntimeCodec.class);
        final Computed computed = varElm.getAnnotation(Computed.class);
        final Counter counter = varElm.getAnnotation(Counter.class);
        final TimeUUID timeUUID = varElm.getAnnotation(TimeUUID.class);
        final ASCII ascii = varElm.getAnnotation(ASCII.class);

        parsingContext.fieldValidator().validateCompatibleCodecAnnotationsOnField(aptUtils, fieldName, className, frozen,
                json, enumerated, codec, runtimeCodec, computed, counter, timeUUID, ascii);

        final List<? extends TypeMirror> nestedTypes = currentType.getKind() == TypeKind.DECLARED ?
                MoreTypes.asDeclared(currentType).getTypeArguments() : Arrays.asList();

        if (isEclipseCompiler(varElm)) {
            final FieldBinding binding = (FieldBinding)((VariableElementImpl) varElm)._binding;

            final List<AnnotationBinding> annotationBindings = Arrays.asList(binding.getAnnotations());

            final Map<Class<? extends Annotation>, TypedMap> annotationInfo = annotationBindings
                .stream()
                .filter(annotBinding -> {
                    final String annotationName = annotBinding.getAnnotationType().debugName();
                    return JSON.class.getCanonicalName().equals(annotationName) ||
                            EmptyCollectionIfNull.class.getCanonicalName().equals(annotationName) ||
                            Enumerated.class.getCanonicalName().equals(annotationName) ||
                            Frozen.class.getCanonicalName().equals(annotationName) ||
                            Computed.class.getCanonicalName().equals(annotationName) ||
                            Counter.class.getCanonicalName().equals(annotationName) ||
                            TimeUUID.class.getCanonicalName().equals(annotationName) ||
                            ASCII.class.getCanonicalName().equals(annotationName) ||
                            Codec.class.getCanonicalName().equals(annotationName) ||
                            RuntimeCodec.class.getCanonicalName().equals(annotationName) ||
                            Index.class.getCanonicalName().equals(annotationName) ||
                            SASI.class.getCanonicalName().equals(annotationName) ||
                            DSE_Search.class.getCanonicalName().equals(annotationName) ||
                            PartitionKey.class.getCanonicalName().equals(annotationName) ||
                            ClusteringColumn.class.getCanonicalName().equals(annotationName);
                })
                .map(x -> inspectSupportedAnnotation_Ecj(aptUtils, currentType, x))
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

            final AnnotationTree annotationTree = new AnnotationTree(currentType, annotationInfo, 1);
            List<TypeBinding> typeBindings = new ArrayList<>();
            if (binding.type instanceof ParameterizedTypeBinding) {
                typeBindings = Arrays.asList(((ParameterizedTypeBinding) binding.type).typeArguments());
            }

            buildTree_Ecj(aptUtils, annotationTree, 1, nestedTypes, typeBindings);
            return annotationTree;
        } else if (isJavaCompiler(varElm)) {
            final Map<Class<? extends Annotation>, TypedMap> annotationInfo = varElm.getAnnotationMirrors()
                    .stream()
                    .filter(x ->
                                    areSameByClass(x, JSON.class) ||
                                    areSameByClass(x, EmptyCollectionIfNull.class) ||
                                    areSameByClass(x, Enumerated.class) ||
                                    areSameByClass(x, Frozen.class) ||
                                    areSameByClass(x, Computed.class) ||
                                    areSameByClass(x, Counter.class) ||
                                    areSameByClass(x, TimeUUID.class) ||
                                    areSameByClass(x, ASCII.class) ||
                                    areSameByClass(x, Codec.class) ||
                                    areSameByClass(x, RuntimeCodec.class) ||
                                    areSameByClass(x, Index.class) ||
                                    areSameByClass(x, SASI.class) ||
                                    areSameByClass(x, DSE_Search.class) ||
                                    areSameByClass(x, PartitionKey.class) ||
                                    areSameByClass(x, ClusteringColumn.class)
                    )
                    .map(x -> (AnnotationMirror) x)
                    .collect(Collectors.toMap(x -> toAnnotation_Javac(aptUtils, x),
                            x -> inspectSupportedAnnotation_Javac(aptUtils, currentType, x)));

            final AnnotationTree annotationTree = new AnnotationTree(currentType, annotationInfo, 1);

            final SymbolMetadata metadata = ((Symbol.VarSymbol) varElm).getMetadata();

            final List<Attribute.TypeCompound> typeAttributes = metadata == null ?
                    Arrays.asList() : metadata.getTypeAttributes();
            buildTree_Javac(aptUtils, annotationTree, 1, nestedTypes, typeAttributes);
            return annotationTree;
        } else {
            aptUtils.printError("Unknown compiler, only standard Java compiler and Eclipse ECJ compiler are supported");
            return null;
        }

    }

    private static AnnotationTree buildTree_Javac(AptUtils aptUtils, AnnotationTree annotationTree, int depth,
                                                  List<? extends TypeMirror> nestedTypes, List<Attribute.TypeCompound> typeAttributes) {

        final TypeMirror currentType = annotationTree.currentType;

        final boolean hasJson = containsAnnotation(annotationTree, JSON.class);

        if (hasJson) {
            return annotationTree;
        }

        if (isPrimitive(currentType) || isArray(currentType) || isAnEnum(currentType) || nestedTypes.size() == 0) {
            return annotationTree;
        } else if (aptUtils.isAssignableFrom(Tuple1.class, currentType) ||
                aptUtils.isAssignableFrom(List.class, currentType) ||
                aptUtils.isAssignableFrom(Set.class, currentType) ||
                aptUtils.isAssignableFrom(java.util.Optional.class, currentType)) {

            final TypeMirror typeMirror = nestedTypes.get(0);
            final Map<Class<? extends Annotation>, TypedMap> annotationsInfo = typeAttributes
                    .stream()
                    .filter(x -> x.getPosition().location.size() == depth)
                    .collect(Collectors.toMap(x -> toAnnotation_Javac(aptUtils, x),
                            x -> inspectSupportedAnnotation_Javac(aptUtils, currentType, x)));

            final AnnotationTree newTree = annotationTree.addNext(new AnnotationTree(typeMirror, annotationsInfo, depth + 1));

            final List<Attribute.TypeCompound> newTypeAttributes = typeAttributes
                    .stream()
                    .filter(x -> x.getPosition().location.size() != depth)
                    .collect(toList());

            return buildTree_Javac(aptUtils, newTree, depth + 1, getTypeArguments(typeMirror), newTypeAttributes);

        } else if (aptUtils.isAssignableFrom(Tuple2.class, currentType) || aptUtils.isAssignableFrom(Map.class, annotationTree.currentType)) {
            return buildTreeForTuple_Javac(aptUtils, annotationTree, depth, 2, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple3.class, currentType)) {
            return buildTreeForTuple_Javac(aptUtils, annotationTree, depth, 3, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple4.class, currentType)) {
            return buildTreeForTuple_Javac(aptUtils, annotationTree, depth, 4, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple5.class, currentType)) {
            return buildTreeForTuple_Javac(aptUtils, annotationTree, depth, 5, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple6.class, currentType)) {
            return buildTreeForTuple_Javac(aptUtils, annotationTree, depth, 6, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple7.class, currentType)) {
            return buildTreeForTuple_Javac(aptUtils, annotationTree, depth, 7, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple8.class, currentType)) {
            return buildTreeForTuple_Javac(aptUtils, annotationTree, depth, 8, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple9.class, currentType)) {
            return buildTreeForTuple_Javac(aptUtils, annotationTree, depth, 9, nestedTypes, typeAttributes);
        } else if (aptUtils.isAssignableFrom(Tuple10.class, currentType)) {
            return buildTreeForTuple_Javac(aptUtils, annotationTree, depth, 10, nestedTypes, typeAttributes);
        } else if (aptUtils.getAnnotationOnClass(currentType, UDT.class).isPresent()) {
            return annotationTree;
        } else if (nestedTypes.size() == 0) {
            return annotationTree;
        } else {
            throw new IllegalStateException("Unknown current type : " + currentType.toString());
        }
    }

    private static AnnotationTree buildTree_Ecj(AptUtils aptUtils, AnnotationTree annotationTree, int depth,
                                                List<? extends TypeMirror> nestedTypes, List<TypeBinding> typeBindings) {
        final TypeMirror currentType = annotationTree.currentType;

        if (containsAnnotation(annotationTree, JSON.class)) {
            return annotationTree;
        }

        if (isPrimitive(currentType) || isArray(currentType) || isAnEnum(currentType) || nestedTypes.size() == 0) {
            return annotationTree;
        } else if (aptUtils.isAssignableFrom(Tuple1.class, currentType) ||
                aptUtils.isAssignableFrom(List.class, currentType) ||
                aptUtils.isAssignableFrom(Set.class, currentType) ||
                aptUtils.isAssignableFrom(java.util.Optional.class, currentType)) {

            final TypeMirror typeMirror = nestedTypes.get(0);
            final TypeBinding nestedTypeBinding = typeBindings.get(0);

            final Map<Class<? extends Annotation>, TypedMap> annotationsInfo = Arrays.asList(nestedTypeBinding.getTypeAnnotations())
                    .stream()
                    .map(annotBinding -> inspectSupportedAnnotation_Ecj(aptUtils, currentType, annotBinding))
                    .collect(Collectors.toMap(pair -> pair._1(), pair -> pair._2()));
            final AnnotationTree newTree = annotationTree.addNext(new AnnotationTree(typeMirror, annotationsInfo, depth + 1));
            List<TypeBinding> nestedBindings = new ArrayList<>();
            if (nestedTypeBinding instanceof ParameterizedTypeBinding) {
                nestedBindings = Arrays.asList(((ParameterizedTypeBinding) nestedTypeBinding).typeArguments());
            }
            return buildTree_Ecj(aptUtils, newTree, depth + 1, getTypeArguments(typeMirror), nestedBindings);
        }  else if (aptUtils.isAssignableFrom(Tuple2.class, currentType) || aptUtils.isAssignableFrom(Map.class, annotationTree.currentType)) {
            return buildTreeForTuple_Ecj(aptUtils, currentType, annotationTree, depth, 2, nestedTypes, typeBindings);
        } else if (aptUtils.isAssignableFrom(Tuple3.class, currentType)) {
            return buildTreeForTuple_Ecj(aptUtils, currentType, annotationTree, depth, 3, nestedTypes, typeBindings);
        } else if (aptUtils.isAssignableFrom(Tuple4.class, currentType)) {
            return buildTreeForTuple_Ecj(aptUtils, currentType, annotationTree, depth, 4, nestedTypes, typeBindings);
        } else if (aptUtils.isAssignableFrom(Tuple5.class, currentType)) {
            return buildTreeForTuple_Ecj(aptUtils, currentType, annotationTree, depth, 5, nestedTypes, typeBindings);
        } else if (aptUtils.isAssignableFrom(Tuple6.class, currentType)) {
            return buildTreeForTuple_Ecj(aptUtils, currentType, annotationTree, depth, 6, nestedTypes, typeBindings);
        } else if (aptUtils.isAssignableFrom(Tuple7.class, currentType)) {
            return buildTreeForTuple_Ecj(aptUtils, currentType, annotationTree, depth, 7, nestedTypes, typeBindings);
        } else if (aptUtils.isAssignableFrom(Tuple8.class, currentType)) {
            return buildTreeForTuple_Ecj(aptUtils, currentType, annotationTree, depth, 8, nestedTypes, typeBindings);
        } else if (aptUtils.isAssignableFrom(Tuple9.class, currentType)) {
            return buildTreeForTuple_Ecj(aptUtils, currentType, annotationTree, depth, 9, nestedTypes, typeBindings);
        } else if (aptUtils.isAssignableFrom(Tuple10.class, currentType)) {
            return buildTreeForTuple_Ecj(aptUtils, currentType, annotationTree, depth, 10, nestedTypes, typeBindings);
        } else if (aptUtils.getAnnotationOnClass(currentType, UDT.class).isPresent()) {
            return annotationTree;
        } else {
            throw new IllegalStateException("Unknown current type : " + currentType.toString());
        }
    }

    private static List<? extends TypeMirror> getTypeArguments(TypeMirror typeMirror) {
        if (isPrimitive(typeMirror) || isArray(typeMirror)) {
            return Collections.emptyList();
        } else {
            return MoreTypes.asDeclared(typeMirror).getTypeArguments();
        }
    }

    private static AnnotationTree buildTreeForTuple_Javac(AptUtils aptUtils, AnnotationTree annotationTree,
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
            final TypeMirror currentType = recursiveTreeN.currentType;
            final Map<Class<? extends Annotation>, TypedMap> annotsN = annotations
                    .stream()
                    .filter(x -> x.getPosition().location.get(depth - 1).arg == j)
                    .collect(Collectors.toMap(x -> toAnnotation_Javac(aptUtils, x),
                            x -> inspectSupportedAnnotation_Javac(aptUtils, currentType, x)));

            newTreeN = recursiveTreeN.addNext(new AnnotationTree(typeMirrorN, annotsN, depth + 1));
            recursiveTreeN = buildTree_Javac(aptUtils, newTreeN, depth + 1, getTypeArguments(typeMirrorN), newTypeAttributes);
        }
        return recursiveTreeN;
    }

    private static AnnotationTree buildTreeForTuple_Ecj(AptUtils aptUtils, TypeMirror currentType, AnnotationTree annotationTree,
                                                        int depth, int cardinality, List<? extends TypeMirror> nestedTypes, List<TypeBinding> typeBindings) {


        AnnotationTree newTreeN;
        AnnotationTree recursiveTreeN = annotationTree;

        for (int i = 0; i < cardinality; i++) {
            final TypeMirror typeMirrorN = nestedTypes.get(i);
            final TypeBinding typeBinding = typeBindings.get(i);
            final Map<Class<? extends Annotation>, TypedMap> annotationsInfo = Arrays.asList(typeBinding.getTypeAnnotations())
                    .stream()
                    .map(annotBinding -> inspectSupportedAnnotation_Ecj(aptUtils, currentType, annotBinding))
                    .collect(Collectors.toMap(pair -> pair._1(), pair -> pair._2()));
            List<TypeBinding> nestedBindings = new ArrayList<>();
            if (typeBinding instanceof ParameterizedTypeBinding) {
                nestedBindings = Arrays.asList(((ParameterizedTypeBinding) typeBinding).typeArguments());
            }
            newTreeN = recursiveTreeN.addNext(new AnnotationTree(typeMirrorN, annotationsInfo, depth + 1));
            recursiveTreeN = buildTree_Ecj(aptUtils, newTreeN, depth + 1, getTypeArguments(typeMirrorN), nestedBindings);
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
            throw new IllegalStateException(String.format("No more leaf for annotation tree. Current type = %s, depth = %s",
                    currentType.toString(), depth));
        }
    }

    public Map<Class<? extends Annotation>, TypedMap> getAnnotations() {
        return annotations;
    }

    public TypeMirror getCurrentType() {
        return currentType;
    }

    public int depth() {
        return this.depth;
    }

    private static TypedMap inspectSupportedAnnotation_Javac(AptUtils aptUtils, TypeMirror currentType, AnnotationMirror annotation) {
        final TypedMap typedMap = new TypedMap();
        if(areSameByClass(annotation, Enumerated.class)){
            final Enumerated.Encoding encoding = getElementValueEnum(annotation, "value", Enumerated.Encoding.class, true);
            return TypedMap.of("value", encoding);
        } else if (areSameByClass(annotation, Codec.class)) {
            final CodecContext codecContext = CodecFactory.buildCodecContext(aptUtils, annotation);
            return TypedMap.of("codecContext", codecContext);
        } else if (areSameByClass(annotation, RuntimeCodec.class)) {
            final RuntimeCodecContext runtimeCodecContext = CodecFactory.buildRuntimeCodecContext(currentType, annotation);
            return TypedMap.of("runtimeCodecContext", runtimeCodecContext);
        } else if (areSameByClass(annotation, Computed.class)) {
            final String function = getElementValue(annotation, "function", String.class, false);
            final Optional<Class<Object>> cqlClass = getElementValueClass(annotation, "cqlClass", false);
            aptUtils.validateTrue(cqlClass.isPresent(), "Cannot find 'cqlClass' attribute value on annotation %s", Computed.class.getCanonicalName());
            String alias = getElementValue(annotation, "alias", String.class, false);
            final List<String> targetColumns = getElementValueArray(annotation, "targetColumns", String.class, false);
            typedMap.put("function", function);
            typedMap.put("cqlClass", cqlClass.get());
            typedMap.put("alias", alias);
            typedMap.put("targetColumns", targetColumns);
            return typedMap;
        } else if (areSameByClass(annotation, Index.class)) {
            final String indexName = getElementValue(annotation, "name", String.class, true);
            final String indexOptions = getElementValue(annotation, "indexOptions", String.class, true);
            final String indexClassName = getElementValue(annotation, "indexClassName", String.class, true);
            typedMap.put("indexInfoContext", new IndexInfoContext(indexName, indexClassName, indexOptions));
            return typedMap;
        } else if(areSameByClass(annotation, SASI.class)) {
            final String indexName = getElementValue(annotation, "name", String.class, true);
            final IndexMode indexMode = getElementValueEnum(annotation, "indexMode", IndexMode.class, true);
            final boolean analyzed = getElementValue(annotation, "analyzed", Boolean.class, true);
            final Analyzer analyzerClass = getElementValueEnum(annotation, "analyzerClass", Analyzer.class, true);
            final int maxCompactionFlushMemoryInMb = getElementValue(annotation, "maxCompactionFlushMemoryInMb", Integer.class, true);
            final Normalization normalization = getElementValueEnum(annotation, "normalization", Normalization.class, true);
            final String locale = getElementValue(annotation, "locale", String.class, true);
            final boolean enableStemming = getElementValue(annotation, "enableStemming", Boolean.class, true);
            final boolean skipStopWords = getElementValue(annotation, "skipStopWords", Boolean.class, true);
            typedMap.put("sasiInfoContext", new SASIInfoContext(indexName, indexMode, analyzed, analyzerClass, maxCompactionFlushMemoryInMb, normalization, locale, enableStemming, skipStopWords));
            return typedMap;
        } else if(areSameByClass(annotation, DSE_Search.class)) {
            final boolean fullTextSearchEnabled = getElementValue(annotation, "fullTextSearchEnabled", Boolean.class, true);
            typedMap.put("dseSearchInfoContext", new DSESearchInfoContext(fullTextSearchEnabled));
            return typedMap;
        } else if (areSameByClass(annotation, PartitionKey.class)) {
            typedMap.put("order", getElementValue(annotation, "value", Integer.class, true));
            return typedMap;
        } else if (areSameByClass(annotation, ClusteringColumn.class)) {
            typedMap.put("order", getElementValue(annotation, "value", Integer.class, true));
            typedMap.put("asc", getElementValue(annotation, "asc", Boolean.class, true));
            return typedMap;
        } else {
            return typedMap;
        }
    }

    private static Class<? extends Annotation> toAnnotation_Javac(AptUtils aptUtils, AnnotationMirror annotationMirror) {
        if (areSameByClass(annotationMirror, JSON.class)) {
            return JSON.class;
        } else if (areSameByClass(annotationMirror, EmptyCollectionIfNull.class)) {
            return EmptyCollectionIfNull.class;
        } else if (areSameByClass(annotationMirror, Enumerated.class)) {
            return Enumerated.class;
        } else if (areSameByClass(annotationMirror, Frozen.class)) {
            return Frozen.class;
        } else if (areSameByClass(annotationMirror, Computed.class)) {
            return Computed.class;
        } else if (areSameByClass(annotationMirror, Counter.class)) {
            return Counter.class;
        } else if (areSameByClass(annotationMirror, TimeUUID.class)) {
            return TimeUUID.class;
        } else if (areSameByClass(annotationMirror, ASCII.class)) {
            return ASCII.class;
        } else if (areSameByClass(annotationMirror, Codec.class)) {
            return Codec.class;
        } else if (areSameByClass(annotationMirror, RuntimeCodec.class)) {
            return RuntimeCodec.class;
        } else if (areSameByClass(annotationMirror, Index.class)) {
            return Index.class;
        } else if (areSameByClass(annotationMirror, SASI.class)) {
            return SASI.class;
        } else if (areSameByClass(annotationMirror, DSE_Search.class)) {
            return DSE_Search.class;
        } else if (areSameByClass(annotationMirror, PartitionKey.class)) {
            return PartitionKey.class;
        } else if (areSameByClass(annotationMirror, ClusteringColumn.class)) {
            return ClusteringColumn.class;
        } else {
            aptUtils.printError("Unsupported annotation : " + annotationMirror.toString());
            throw new IllegalArgumentException("Unsupported annotation : " + annotationMirror.toString());
        }
    }

    private static Tuple2<Class<? extends Annotation>, TypedMap> inspectSupportedAnnotation_Ecj(AptUtils aptUtils,
                                                                                                TypeMirror currentType,
                                                                                                AnnotationBinding annotationBinding) {
        final TypedMap typedMap = new TypedMap();
        final String annotationName = annotationBinding.getAnnotationType().debugName();
        if (JSON.class.getCanonicalName().equals(annotationName)) {
            return Tuple2.of(JSON.class, typedMap);
        } else if (EmptyCollectionIfNull.class.getCanonicalName().equals(annotationName)) {
            return Tuple2.of(EmptyCollectionIfNull.class, typedMap);
        } else if (Enumerated.class.getCanonicalName().equals(annotationName)) {
            final Enumerated.Encoding encoding = Arrays.asList(annotationBinding.getElementValuePairs())
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("value"))
                    .map(pair -> pair.getValue())
                    .filter(value -> value instanceof FieldBinding)
                    .map(value -> (FieldBinding) value)
                    .filter(value -> Enumerated.Encoding.class.getCanonicalName().equals(value.type.debugName()))
                    .map(value -> Enumerated.Encoding.valueOf(Enumerated.Encoding.class, new String(value.name)))
                    .findFirst()
                    .orElse(Enumerated.Encoding.NAME);
            typedMap.put("value", encoding);
            return Tuple2.of(Enumerated.class, typedMap);
        } else if (Frozen.class.getCanonicalName().equals(annotationName)) {
            return Tuple2.of(Frozen.class, typedMap);
        } else if (Computed.class.getCanonicalName().equals(annotationName)) {
            final List<ElementValuePair> pairs = Arrays.asList(annotationBinding.getElementValuePairs());
            final String functionName = ((StringConstant) pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("function"))
                    .findFirst().get()
                    .getValue()).stringValue();

            final String alias = ((StringConstant) pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("alias"))
                    .findFirst().get()
                    .getValue()).stringValue();

            final String cqlClassName = ((ReferenceBinding) pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("cqlClass"))
                    .findFirst().get()
                    .getValue()).debugName();

            Class<?> cqlClass = null;
            try {
                cqlClass = Class.forName(cqlClassName);
            } catch (ClassNotFoundException e) {
                aptUtils.printError("Cannot find CQL class %s", cqlClassName);
            }

            final List<String> targetColumns = Arrays.asList((Object[]) pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("targetColumns"))
                    .findFirst().get()
                    .getValue())
                    .stream()
                    .map(object -> (StringConstant) object)
                    .map(stringConstant -> stringConstant.stringValue())
                    .collect(toList());

            typedMap.put("function", functionName);
            typedMap.put("alias", alias);
            typedMap.put("cqlClass", cqlClass);
            typedMap.put("targetColumns", targetColumns);
            return Tuple2.of(Computed.class, typedMap);
        } else if (Counter.class.getCanonicalName().equals(annotationName)) {
            return Tuple2.of(Counter.class, typedMap);
        } else if (TimeUUID.class.getCanonicalName().equals(annotationName)) {
            return Tuple2.of(TimeUUID.class, typedMap);
        } else if (ASCII.class.getCanonicalName().equals(annotationName)) {
            return Tuple2.of(ASCII.class, typedMap);
        } else if (Codec.class.getCanonicalName().equals(annotationName)) {
            final Optional<String> codecClassName = Arrays.asList(annotationBinding.getElementValuePairs())
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("value"))
                    .map(pair -> pair.getValue())
                    .filter(value -> value instanceof ReferenceBinding)
                    .map(value -> ((ReferenceBinding)value).debugName())
                    .findFirst();
            aptUtils.validateTrue(codecClassName.isPresent(),
                    "Cannot find codec class on '%s' for type '%s",
                    Codec.class.getCanonicalName(),
                    currentType);

            final CodecContext codecContext = CodecFactory.buildCodecContext(aptUtils, codecClassName.get());
            typedMap.put("codecContext", codecContext);
            return Tuple2.of(Codec.class, typedMap);

        } else if (RuntimeCodec.class.getCanonicalName().equals(annotationName)) {

            final List<ElementValuePair> pairs = Arrays.asList(annotationBinding.getElementValuePairs());
            final Optional<String> codecName = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("codecName"))
                    .findFirst()
                    .map(x -> (StringConstant) x.getValue())
                    .map(x -> x.stringValue());

            final String targetTypeName = ((ReferenceBinding) pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("cqlClass"))
                    .findFirst().get()
                    .getValue()).debugName();

            Class<?> targetType = null;
            try {
                targetType = Class.forName(targetTypeName);
            } catch (ClassNotFoundException e) {
                aptUtils.printError("Cannot find CQL class %s", targetTypeName);
            }

            final RuntimeCodecContext runtimeCodecContext = new RuntimeCodecContext(
                    TypeName.get(currentType), TypeName.get(targetType), codecName);
            typedMap.put("runtimeCodecContext", runtimeCodecContext);
            return Tuple2.of(RuntimeCodec.class, typedMap);

        } else if (Index.class.getCanonicalName().equals(annotationName)) {
            final List<ElementValuePair> pairs = Arrays.asList(annotationBinding.getElementValuePairs());
            final String name = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("name"))
                    .map(pair -> ((StringConstant) pair.getValue()).stringValue())
                    .findFirst().orElse("");

            final String indexClassName = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("indexClassName"))
                    .map(pair -> ((StringConstant) pair.getValue()).stringValue())
                    .findFirst().orElse("");

            final String indexOptions = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("indexOptions"))
                    .map(pair -> ((StringConstant) pair.getValue()).stringValue())
                    .findFirst().orElse("");

            typedMap.put("indexInfoContext", new IndexInfoContext(name, indexClassName, indexOptions));
            return Tuple2.of(Index.class, typedMap);
        } else if (SASI.class.getCanonicalName().equals(annotationName)) {

            final List<ElementValuePair> pairs = Arrays.asList(annotationBinding.getElementValuePairs());

            final String indexName = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("name"))
                    .map(pair -> ((StringConstant) pair.getValue()).stringValue())
                    .findFirst().orElse("");

            final IndexMode indexMode = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("indexMode"))
                    .map(pair -> pair.getValue())
                    .filter(value -> value instanceof FieldBinding)
                    .map(value -> (FieldBinding) value)
                    .filter(value -> IndexMode.class.getCanonicalName().equals(value.type.debugName()))
                    .map(value -> IndexMode.valueOf(IndexMode.class, new String(value.name)))
                    .findFirst()
                    .orElse(IndexMode.PREFIX);

            final boolean analyzed = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("analyzed"))
                    .map(pair -> ((BooleanConstant) pair.getValue()).booleanValue())
                    .findFirst().orElse(false);

            final Analyzer analyzerClass = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("analyzerClass"))
                    .map(pair -> pair.getValue())
                    .filter(value -> value instanceof FieldBinding)
                    .map(value -> (FieldBinding) value)
                    .filter(value -> Analyzer.class.getCanonicalName().equals(value.type.debugName()))
                    .map(value -> Analyzer.valueOf(Analyzer.class, new String(value.name)))
                    .findFirst()
                    .orElse(Analyzer.NO_OP_ANALYZER);

            final int maxCompactionFlushMemoryInMb = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("maxCompactionFlushMemoryInMb"))
                    .map(pair -> ((IntConstant) pair.getValue()).intValue())
                    .findFirst().orElse(1024);

            final Normalization normalization = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("normalization"))
                    .map(pair -> pair.getValue())
                    .filter(value -> value instanceof FieldBinding)
                    .map(value -> (FieldBinding) value)
                    .filter(value -> Normalization.class.getCanonicalName().equals(value.type.debugName()))
                    .map(value -> Analyzer.valueOf(Normalization.class, new String(value.name)))
                    .findFirst()
                    .orElse(Normalization.NONE);

            final String locale = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("locale"))
                    .map(pair -> ((StringConstant) pair.getValue()).stringValue())
                    .findFirst().orElse("en");

            final boolean enableStemming = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("enableStemming"))
                    .map(pair -> ((BooleanConstant) pair.getValue()).booleanValue())
                    .findFirst().orElse(false);

            final boolean skipStopWords = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("skipStopWords"))
                    .map(pair -> ((BooleanConstant) pair.getValue()).booleanValue())
                    .findFirst().orElse(false);

            typedMap.put("sasiInfoContext", new SASIInfoContext(indexName, indexMode, analyzed, analyzerClass, maxCompactionFlushMemoryInMb, normalization, locale, enableStemming, skipStopWords));
            return Tuple2.of(SASI.class, typedMap);
        } else if (DSE_Search.class.getCanonicalName().equals(annotationName)) {
            final List<ElementValuePair> pairs = Arrays.asList(annotationBinding.getElementValuePairs());
            final boolean fullTextSearchEnabled = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("fullTextSearchEnabled"))
                    .map(pair -> ((BooleanConstant) pair.getValue()).booleanValue())
                    .findFirst().orElse(false);
            typedMap.put("dseSearchInfoContext", new DSESearchInfoContext(fullTextSearchEnabled));
            return Tuple2.of(DSE_Search.class, typedMap);
        } else if (PartitionKey.class.getCanonicalName().equals(annotationName)) {
            final List<ElementValuePair> pairs = Arrays.asList(annotationBinding.getElementValuePairs());
            final Integer order = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("value"))
                    .map(pair -> ((IntConstant) pair.getValue()).intValue())
                    .findFirst().orElse(1);
            typedMap.put("order", order);
            return Tuple2.of(PartitionKey.class, typedMap);
        } else if (ClusteringColumn.class.getCanonicalName().equals(annotationName)) {
            final List<ElementValuePair> pairs = Arrays.asList(annotationBinding.getElementValuePairs());
            final Integer order = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("value"))
                    .map(pair -> ((IntConstant) pair.getValue()).intValue())
                    .findFirst().orElse(1);
            final Boolean asc = pairs
                    .stream()
                    .filter(pair -> new String(pair.getName()).equals("asc"))
                    .map(pair -> ((BooleanConstant)pair.getValue()).booleanValue())
                    .findFirst().orElse(true);

            typedMap.put("order", order);
            typedMap.put("asc", asc);
            return Tuple2.of(ClusteringColumn.class, typedMap);
        } else {
            aptUtils.printError("Unsupported annotation : " + annotationName);
            throw new IllegalArgumentException("Unsupported annotation : " + annotationName.toString());
        }
    }

    public static Optional<TypeName> findOptionalViewBaseClass(AptUtils aptUtils, TypeElement elm) {
        if (AptUtils.isJavaCompiler(elm)) {
            return findOptionalViewBaseClass_Javac(aptUtils, elm);
        } else if (AptUtils.isEclipseCompiler(elm)) {
            return findOptionalViewBaseClass_Ecj(elm);
        } else {
            aptUtils.printError("Unknown compiler, only standard Java compiler and Eclipse ECJ compiler are supported");
            return Optional.empty();
        }
    }

    private static Optional<TypeName> findOptionalViewBaseClass_Javac(AptUtils aptUtils, TypeElement elm) {
        Optional<TypeName> viewBaseClass = elm.getAnnotationMirrors()
                .stream()
                .filter(x -> areSameByClass(x, MaterializedView.class))
                .findFirst()
                .flatMap(view -> aptUtils.getElementValueClass(view, "baseEntity", false))
                .map(ClassName::get);

        if (viewBaseClass.isPresent()) {
            return viewBaseClass;
        } else {
            return elm.getAnnotationMirrors()
                    .stream()
                    .filter(x -> areSameByClass(x, MaterializedView.class))
                    .findFirst()
                    .map(view -> aptUtils.getElementValueClassName(view, "baseEntity", false).toString())
                    .map(baseClassName ->aptUtils.elementUtils.getTypeElement(baseClassName).asType())
                    .map(ClassName::get);
        }
    }

    private static Optional<TypeName> findOptionalViewBaseClass_Ecj(TypeElement elm) {
        return Arrays.asList(((TypeElementImpl) elm)._binding.getAnnotations())
                .stream()
                .filter(annotBinding -> MaterializedView.class.getCanonicalName().equals(annotBinding.getAnnotationType().debugName()))
                .flatMap(viewAnnot -> Arrays.asList(viewAnnot.getElementValuePairs()).stream())
                .filter(pair -> new String(pair.getName()).equals("baseEntity"))
                .filter(pair -> pair.getValue() instanceof ReferenceBinding)
                .map(pair -> (ReferenceBinding)pair.getValue())
                .map(x -> x.debugName())
                .map(ClassName::bestGuess)
                .map(x -> (TypeName)x)
                .findFirst();
    }

    public static Optional<String> findKeyspaceForFunctionRegistry(AptUtils aptUtils, TypeElement functionRegistry) {
        if (isJavaCompiler(functionRegistry)) {
            return functionRegistry.getAnnotationMirrors()
                    .stream()
                    .filter(x -> areSameByClass(x, FunctionRegistry.class))
                    .findFirst()
                    .map(annot -> aptUtils.getElementValue(annot, "keyspace", String.class, false));

        } else if (AptUtils.isEclipseCompiler(functionRegistry)) {
            return Arrays.asList(((TypeElementImpl) functionRegistry)._binding.getAnnotations())
                    .stream()
                    .filter(annotBinding -> FunctionRegistry.class.getCanonicalName().equals(annotBinding.getAnnotationType().debugName()))
                    .flatMap(annot -> Arrays.asList(annot.getElementValuePairs()).stream())
                    .filter(pair -> new String(pair.getName()).equals("keyspace"))
                    .findFirst()
                    .map(elementValuePair -> (StringConstant)elementValuePair.getValue())
                    .map(stringConstant -> stringConstant.stringValue());
        } else {
            aptUtils.printError("Unknown compiler, only standard Java compiler and Eclipse ECJ compiler are supported");
            return Optional.empty();
        }
    }
}
