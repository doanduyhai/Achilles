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

package info.archinnov.achilles.internals.apt;

import static com.google.auto.common.MoreTypes.asDeclared;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import org.eclipse.jdt.internal.compiler.apt.model.ExecutableElementImpl;
import org.eclipse.jdt.internal.compiler.apt.model.TypeElementImpl;
import org.eclipse.jdt.internal.compiler.apt.model.VariableElementImpl;

import com.datastax.driver.core.UDTValue;
import com.google.auto.common.MoreElements;
import com.google.auto.common.MoreTypes;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.code.Type;
import com.sun.tools.javac.model.JavacElements;

import info.archinnov.achilles.annotations.UDT;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internals.parser.AnnotationTree;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.Tuple2;

/**
 * Utility methods for compile time type handling
 * Some methods are borrowed from the Checker Framework : http://types.cs.washington.edu/checker-framework/
 */
public class AptUtils {

    private static boolean HAS_JAVAC_CLASSES = false;
    private static boolean HAS_ECJ_CLASSES = false;

    static {
        try {
            Class.forName("com.sun.tools.javac.code.Symbol");
            HAS_JAVAC_CLASSES = true;
        } catch (ClassNotFoundException e) {
            HAS_JAVAC_CLASSES = false;
        }

        try {
            Class.forName("org.eclipse.jdt.internal.compiler.apt.model.TypeElementImpl");
            HAS_ECJ_CLASSES = true;
        } catch (ClassNotFoundException e) {
            HAS_ECJ_CLASSES = false;
        }
    }

    public final Elements elementUtils;
    public final Types typeUtils;
    public final Messager messager;
    public final Filer filer;

    public AptUtils(Elements elementUtils, Types typeUtils, Messager messager, Filer filer) {
        this.elementUtils = elementUtils;
        this.typeUtils = typeUtils;
        this.messager = messager;
        this.filer = filer;
    }

    public static boolean isPrimitive(TypeMirror typeMirror) {
        return typeMirror.getKind().isPrimitive();
    }

    public static boolean isArray(TypeMirror typeMirror) {
        return typeMirror.getKind() == TypeKind.ARRAY;
    }

    public static boolean isAnEnum(TypeMirror typeMirror) {
        return asDeclared(typeMirror).asElement().getKind() == ElementKind.ENUM;
    }

    public static boolean isAnEnum(Object enumValue) {
        return enumValue.getClass().isEnum();
    }

    public static List<? extends TypeMirror> getTypeArguments(TypeMirror typeMirror) {
        return typeMirror.getKind() == TypeKind.DECLARED ? asDeclared(typeMirror).getTypeArguments() : Arrays.asList();
    }

    public static List<? extends TypeMirror> getInterfaces(TypeMirror typeMirror) {
        return MoreElements.asType(MoreTypes.asElement(typeMirror)).getInterfaces();
    }

    public static boolean containsAnnotation(Set<Class<? extends Annotation>> annotationMirrors, Class<? extends Annotation> annotationClass) {
        return annotationMirrors
                .stream()
                .filter(x -> x.equals(annotationClass))
                .findAny().isPresent();
    }

    public static long countElementsAnnotatedBy(Set<? extends TypeElement> typeElements, Class<? extends Annotation> annotationClass) {
        return typeElements
                .stream()
                .filter(annotation -> isAnnotationOfType(annotation, annotationClass))
                .count();
    }

    public static boolean containsElementsAnnotatedBy(Set<? extends TypeElement> typeElements, Class<? extends Annotation> annotationClass) {
        return countElementsAnnotatedBy(typeElements, annotationClass) > 0;
    }

    public static boolean containsAnnotation(AnnotationTree annotationTree, Class<? extends Annotation> annotationClass) {
        return annotationTree
                .getAnnotations()
                .keySet()
                .stream()
                .filter(x -> x.equals(annotationClass))
                .findAny().isPresent();
    }

    public static List<TypeElement> getTypesAnnotatedBy(Set<? extends TypeElement> annotatedTypes,
        RoundEnvironment roundEnv, Class<? extends  Annotation> annotationClass) {
        return getTypesAnnotatedByAsStream(annotatedTypes, roundEnv, annotationClass)
                .collect(toList());
    }

    public static Stream<TypeElement> getTypesAnnotatedByAsStream(Set<? extends TypeElement> annotatedTypes,
                                                                  RoundEnvironment roundEnv, Class<? extends  Annotation> annotationClass) {
        return annotatedTypes
                .stream()
                .filter(annotation -> isAnnotationOfType(annotation, annotationClass))
                .flatMap(annotation -> roundEnv.getElementsAnnotatedWith(annotationClass).stream())
                .map(MoreElements::asType);
    }

    public static Optional<TypedMap> extractTypedMap(AnnotationTree annotationTree, Class<? extends Annotation> annotationClass) {
        return annotationTree
                .getAnnotations()
                .entrySet()
                .stream()
                .filter(x -> x.getKey().equals(annotationClass))
                .map(Map.Entry::getValue)
                .findFirst();
    }

    public static TypeElement asTypeElement(TypeMirror typeMirror) {
        return MoreTypes.asTypeElement(typeMirror);
    }

    public static boolean isAnnotationOfType(TypeElement element, Class<? extends Annotation> annotationClass) {
        return element.getKind() == ElementKind.ANNOTATION_TYPE
        && element.getQualifiedName().toString().equals(annotationClass.getCanonicalName());

    }
    // INSTANCE METHODS

    /**
     * Checks that the annotation {@code am} has the name of {@code anno}.
     * Values are ignored.
     */
    public static boolean areSameByClass(AnnotationMirror am, Class<? extends Annotation> anno) {
        return areSameByName(am, anno.getCanonicalName().intern());
    }

    /**
     * Checks that the annotation {@code am} has the name {@code aname}. Values
     * are ignored.
     */
    public static boolean areSameByName(AnnotationMirror am, /*@Interned*/ String aname) {
        // Both strings are interned.
        return annotationName(am) == aname;
    }

    /**
     * @return the fully-qualified name of an annotation as a Name
     */
    public static final String annotationName(AnnotationMirror annotation) {
        final TypeElement elm = (TypeElement) annotation.getAnnotationType().asElement();
        return elm.getQualifiedName().toString().intern();
    }

    /**
     * Returns the values of an annotation's attributes, including defaults.
     * The method with the same name in JavacElements cannot be used directly,
     * because it includes a cast to Attribute.Compound, which doesn't hold
     * for annotations generated by the Checker Framework.
     *
     * @param annotMirror annotation to examine
     * @return the values of the annotation's elements, including defaults
     * @see AnnotationMirror#getElementValues()
     * @see JavacElements#getElementValuesWithDefaults(AnnotationMirror)
     */
    public static Map<ExecutableElement, AnnotationValue> getElementValuesWithDefaults(AnnotationMirror annotMirror) {
        Map<ExecutableElement, AnnotationValue> valMap = Optional
                .ofNullable((Map<ExecutableElement, AnnotationValue>)annotMirror.getElementValues())
                .map(x -> new HashMap<>(x))
                .orElse(new HashMap<>());

        ElementFilter.methodsIn(annotMirror.getAnnotationType().asElement().getEnclosedElements())
                .stream()
                .map(annot -> Tuple2.of(annot, annot.getDefaultValue()))
                .filter(tuple2 -> tuple2._2() != null && !valMap.containsKey(tuple2._1()))
                .forEach(tuple2 -> valMap.put(tuple2._1(), tuple2._2()));

        return valMap;
    }

    /**
     * Get the attribute with the name {@code name} of the annotation
     * {@code anno}. The result is expected to have type {@code expectedType}.


     * <em>Note 1</em>: The method does not work well for attributes of an array
     * type (as it would return a list of {@link AnnotationValue}s). Use
     * {@code getElementValueArray} instead.


     * <em>Note 2</em>: The method does not work for attributes of an enum type,
     * as the AnnotationValue is a VarSymbol and would be cast to the enum type,
     * which doesn't work. Use {@code getElementValueEnum} instead.
     *
     * @param anno         the annotation to disassemble
     * @param name         the name of the attribute to access
     * @param expectedType the expected type used to cast the return type
     * @param useDefaults  whether to apply default values to the attribute.
     * @return the value of the attribute with the given name
     */
    public static <T> T getElementValue(AnnotationMirror anno, CharSequence name, Class<T> expectedType, boolean useDefaults) {
        Map<? extends ExecutableElement, ? extends AnnotationValue> valmap
                = useDefaults
                ? getElementValuesWithDefaults(anno)
                : anno.getElementValues();


        for (ExecutableElement elem : valmap.keySet()) {
            if (elem.getSimpleName().contentEquals(name)) {
                AnnotationValue val = valmap.get(elem);
                return expectedType.cast(val.getValue());
            }
        }
        return null;
    }

    /**
     * Get the attribute with the name {@code name} of the annotation
     * {@code anno}, where the attribute has an array type. One element of the
     * result is expected to have type {@code expectedType}.

     * Parameter useDefaults is used to determine whether default values
     * should be used for annotation values. Finding defaults requires
     * more computation, so should be false when no defaulting is needed.
     *
     * @param anno         the annotation to disassemble
     * @param name         the name of the attribute to access
     * @param expectedType the expected type used to cast the return type
     * @param useDefaults  whether to apply default values to the attribute.
     * @return the value of the attribute with the given name
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> getElementValueArray(AnnotationMirror anno, CharSequence name, Class<T> expectedType, boolean useDefaults) {
        return ((List<AnnotationValue>) getElementValue(anno, name, List.class, useDefaults))
                .stream()
                .map(elementVal -> expectedType.cast(elementVal.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Get the Name of the class that is referenced by attribute 'name'.
     * This is a convenience method for the most common use-case.
     * Like getElementValue(anno, name, ClassType.class).getQualifiedName(), but
     * this method ensures consistent use of the qualified name.
     */
    public static Name getElementValueClassName(AnnotationMirror anno, CharSequence name, boolean useDefaults) {
        Type.ClassType ct = getElementValue(anno, name, Type.ClassType.class, useDefaults);
        // TODO:  Is it a problem that this returns the type parameters too?  Should I cut them off?
        return ct.asElement().getQualifiedName();
    }

    /**
     * Get the Class that is referenced by attribute 'name'.
     * This method uses Class.forName to load the class. It returns
     * null if the class wasn't found.
     */
    public static <T> Optional<Class<T>> getElementValueClass(AnnotationMirror anno, CharSequence name,
                                                              boolean useDefaults) {
        Name cn = getElementValueClassName(anno, name, useDefaults);
        try {
            Class<?> cls = Class.forName(cn.toString());
            return Optional.of((Class<T>) cls);
        } catch (ClassNotFoundException e) {
            return Optional.empty();
        }
    }

    /**
     * Version that is suitable for Enum elements.
     */
    public static <T extends Enum<T>> T getElementValueEnum(AnnotationMirror anno, CharSequence name, Class<T> t, boolean useDefaults) {
        Symbol.VarSymbol vs = getElementValue(anno, name, Symbol.VarSymbol.class, useDefaults);
        T value = Enum.valueOf(t, vs.getSimpleName().toString());
        return value;
    }

    /**
     * Returns the innermost type element enclosing the given element
     *
     * @param elem the enclosed element of a class
     * @return the innermost type element
     */
    public static TypeElement enclosingClass(final Element elem) {
        Element result = elem;
        while (result != null && !result.getKind().isClass()
                && !result.getKind().isInterface()) {
            Element encl = result.getEnclosingElement();
            result = encl;
        }
        return (TypeElement) result;
    }

    /**
     * Returns the field of the class
     */
    public static VariableElement findFieldInType(TypeElement type, String name) {
        for (VariableElement field : ElementFilter.fieldsIn(type.getEnclosedElements())) {
            if (field.getSimpleName().toString().equals(name)) {
                return field;
            }
        }
        return null;
    }


    public static boolean isJavaCompiler(VariableElement varElm) {
        return HAS_JAVAC_CLASSES && (varElm instanceof Symbol.VarSymbol);
    }

    public static boolean isJavaCompiler(TypeElement typeElm) {
        return HAS_JAVAC_CLASSES && (typeElm instanceof Symbol.ClassSymbol);
    }

    public static boolean isJavaCompiler(ExecutableElement executableElement) {
        return HAS_JAVAC_CLASSES && (executableElement instanceof Symbol.MethodSymbol);
    }

    public static boolean isEclipseCompiler(VariableElement varElm) {
        return HAS_ECJ_CLASSES && (varElm instanceof VariableElementImpl);
    }

    public static boolean isEclipseCompiler(TypeElement typeElm) {
        return HAS_ECJ_CLASSES && (typeElm instanceof TypeElementImpl);
    }

    public static boolean isEclipseCompiler(ExecutableElement executableElement) {
        return HAS_ECJ_CLASSES && (executableElement instanceof ExecutableElementImpl);
    }

    public static String getShortname(TypeName typeName) {
        return typeName.toString().replaceAll("[^.]+\\.", "");
    }

    /**
     *
     * INSTANCE METHODS
     *
     */

    public <T extends Annotation> Optional<T> getAnnotationOnClass(TypeMirror typeMirror, Class<T> annotationClass) {
        if (typeMirror.getKind() == TypeKind.DECLARED) {
            return Optional.ofNullable(this.elementUtils.getTypeElement(MoreTypes.asDeclared(this.erasure(typeMirror)).asElement().toString())
                    .getAnnotation(annotationClass));
        } else {
            return Optional.empty();
        }
    }

    public <T extends Annotation> Optional<T> getAnnotationOnClass(TypeElement typeElement, Class<T> annotationClass) {
        return Optional.ofNullable(this.elementUtils.getTypeElement(typeElement.getQualifiedName())
                .getAnnotation(annotationClass));
    }

    public TypeName extractTypeArgument(TypeName typeName, int argumentIndex) {
        validateTrue(typeName instanceof ParameterizedTypeName, "Type name %s is not an instance of ParameterizedTypeName", typeName);
        final ParameterizedTypeName paramTypeName = (ParameterizedTypeName) typeName;
        validateTrue(paramTypeName.typeArguments.size() >= argumentIndex + 1,
                "Cannot get '%s' th argument from ParameterizedTypeName '%s' ", argumentIndex, paramTypeName);

        return paramTypeName.typeArguments.get(argumentIndex);
    }

    public boolean isAssignableFrom(Class<?> superClass, TypeMirror typeMirror) {
        final TypeKind kind = typeMirror.getKind();
        switch (kind) {
            case BOOLEAN:
                return superClass.equals(boolean.class);
            case BYTE:
                return superClass.equals(byte.class);
            case SHORT:
                return superClass.equals(short.class);
            case INT:
                return superClass.equals(int.class);
            case LONG:
                return superClass.equals(long.class);
            case CHAR:
                return superClass.equals(char.class);
            case FLOAT:
                return superClass.equals(float.class);
            case DOUBLE:
                return superClass.equals(float.class);
            case DECLARED:
                return typeUtils.isAssignable(asTypeElement(typeMirror).asType(), elementUtils.getTypeElement(superClass.getCanonicalName()).asType());
            case ARRAY:
                final TypeMirror componentType = MoreTypes.asArray(typeMirror).getComponentType();
                final Class<?> componentClass = superClass.getComponentType();
                return componentClass == null ? false : isAssignableFrom(componentClass, componentType);
            default:
                return false;
        }
    }

    public ExecutableElement findGetter(TypeElement classElm, VariableElement elm, List<String> getterNames) {
        TypeMirror typeMirror = elm.asType();
        final Optional<ExecutableElement> getter = ElementFilter.methodsIn(elementUtils.getAllMembers(classElm))
                .stream()
                .filter(x -> getterNames.contains(x.getSimpleName().toString()))
                .filter(x -> typeUtils.isSameType(x.getReturnType(), typeMirror))
                .findFirst();

        validateTrue(getter.isPresent(), "Cannot find getter of names '%s' for field '%s' in class '%s'",
                getterNames, elm.getSimpleName(), classElm.getQualifiedName());
        return getter.get();
    }

    public ExecutableElement findSetter(TypeElement classElm, VariableElement elm, String setterName) {
        TypeMirror typeMirror = elm.asType();
        final Optional<ExecutableElement> setter = ElementFilter.methodsIn(elementUtils.getAllMembers(classElm))
                .stream()
                .filter(x -> x.getSimpleName().contentEquals(setterName))
                .filter(x -> x.getParameters().size() == 1)
                .filter(x -> x.getParameters()
                        .stream()
                        .map(VariableElement::asType)
                        .filter(y -> typeUtils.isSameType(y, typeMirror))
                        .count() == 1)
                .filter(x -> x.getReturnType().getKind() == TypeKind.VOID)
                .findFirst();

        validateTrue(setter.isPresent(), "Cannot find setter 'void %s(%s value)' for field '%s' in class '%s'",
                setterName, typeMirror, elm.getSimpleName(), classElm.getQualifiedName());
        return setter.get();
    }

    public void printError(String message, Object... args) {
        print(Diagnostic.Kind.ERROR, message, args);
    }

    public void printWarning(String message, Object... args) {
        print(Diagnostic.Kind.WARNING, message, args);
    }

    public void printMandatoryWarning(String message, Object... args) {
        print(Diagnostic.Kind.MANDATORY_WARNING, message, args);
    }

    public void printNote(String message, Object... args) {
        print(Diagnostic.Kind.NOTE, message, args);
    }

    public void print(Diagnostic.Kind kind, String message, Object... args) {
        messager.printMessage(kind, String.format(message, args));
    }

    public TypeMirror erasure(TypeElement typeElement) {
        return typeUtils.erasure(typeElement.asType());
    }

    public TypeMirror erasure(TypeMirror typeMirror) {
        return typeUtils.erasure(typeMirror);
    }

    public void validateTrue(boolean condition, String message, Object... params) {
        if (!condition) {
            messager.printMessage(Diagnostic.Kind.ERROR, format(message, params));
            throw new AchillesBeanMappingException(format(message, params));
        }
    }

    public void validateFalse(boolean condition, String message, Object... params) {
        if (condition) {
            messager.printMessage(Diagnostic.Kind.ERROR, format(message, params));
            throw new AchillesBeanMappingException(format(message, params));
        }
    }

    public boolean isCompositeTypeForCassandra(TypeMirror typeMirror) {
        final TypeMirror type = this.erasure(typeMirror);
        return this.isAssignableFrom(List.class, type)
                || this.isAssignableFrom(Set.class, type)
                || this.isAssignableFrom(Map.class, type)
                || this.isAssignableFrom(List.class, type)
                || this.isAssignableFrom(UDTValue.class, type)
                || getAnnotationOnClass(typeMirror, UDT.class).isPresent();
    }
}
