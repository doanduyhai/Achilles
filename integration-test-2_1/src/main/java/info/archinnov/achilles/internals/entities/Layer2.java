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
import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.UDT;

@UDT
public class Layer2 {
    @Column
    private String layer;

    @Frozen
    @Column
    private Layer3 layer3;

    public Layer2(){}

    public Layer2(String layer, Layer3 layer3) {
        this.layer = layer;
        this.layer3 = layer3;
    }

    public String getLayer() {
        return layer;
    }

    public void setLayer(String layer) {
        this.layer = layer;
    }

    public Layer3 getLayer3() {
        return layer3;
    }

    public void setLayer3(Layer3 layer3) {
        this.layer3 = layer3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Layer2 layer2 = (Layer2) o;
        return Objects.equals(layer, layer2.layer) &&
                Objects.equals(layer3, layer2.layer3);
    }

    @Override
    public int hashCode() {
        return Objects.hash(layer, layer3);
    }
}
