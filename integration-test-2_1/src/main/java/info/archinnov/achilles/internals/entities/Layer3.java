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

package info.archinnov.achilles.internals.entities;

import java.util.Objects;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.UDT;

@UDT
public class Layer3 {
    @Column
    private String layer;

    public Layer3(){}

    public Layer3(String layer) {
        this.layer = layer;
    }

    public String getLayer() {
        return layer;
    }

    public void setLayer(String layer) {
        this.layer = layer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Layer3 layer3 = (Layer3) o;
        return Objects.equals(layer, layer3.layer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(layer);
    }
}
