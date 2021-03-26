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

package org.eclipse.jdt.internal.compiler.apt.model;

import org.eclipse.jdt.internal.compiler.lookup.Binding;

/**
 * Ugly hack to access internal field  _binding
 * of TypeMirrorImpl class
 */
public class DeclaredTypeImplContainer {

    private final TypeMirrorImpl typeMirror;

    private DeclaredTypeImplContainer(TypeMirrorImpl typeMirror) {
        this.typeMirror = typeMirror;
    }

    public static DeclaredTypeImplContainer from(TypeMirrorImpl typeMirror) {
        return new DeclaredTypeImplContainer(typeMirror);
    }

    public Binding getBinding() {
        return typeMirror._binding;
    }


}
