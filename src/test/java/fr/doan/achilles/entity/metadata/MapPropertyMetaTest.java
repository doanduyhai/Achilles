package fr.doan.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import fr.doan.achilles.entity.metadata.MapPropertyMeta;

public class MapPropertyMetaTest {

    @Test
    public void should_exception_when_cannot_instanciate() throws Exception {

        MapPropertyMeta<String> mapMeta = new MapPropertyMeta<String>();
        mapMeta.setMapClass(MyMap.class);

        Map<?, String> map = mapMeta.newMapInstance();

        assertThat(map).isInstanceOf(HashMap.class);

    }

    class MyMap<K, V> implements Map<K, V> {

        private MyMap() {
        }

        @Override
        public int size() {

            return 0;
        }

        @Override
        public boolean isEmpty() {

            return false;
        }

        @Override
        public boolean containsKey(Object key) {

            return false;
        }

        @Override
        public boolean containsValue(Object value) {

            return false;
        }

        @Override
        public V get(Object key) {

            return null;
        }

        @Override
        public V put(K key, V value) {

            return null;
        }

        @Override
        public V remove(Object key) {

            return null;
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {

        }

        @Override
        public void clear() {

        }

        @Override
        public Set<K> keySet() {

            return null;
        }

        @Override
        public Collection<V> values() {

            return null;
        }

        @Override
        public Set<java.util.Map.Entry<K, V>> entrySet() {

            return null;
        }

    }
}
