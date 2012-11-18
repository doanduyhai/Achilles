package fr.doan.achilles.metadata;

import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.junit.Test;

public class SetPropertyMetaTest {

    @Test
    public void should_exception_when_cannot_instanciate() throws Exception {

        SetPropertyMeta<String> setMeta = new SetPropertyMeta<String>();
        setMeta.setSetClass(MySet.class);

        Set<String> set = setMeta.newSetInstance();

        assertThat(set).isInstanceOf(HashSet.class);
    }

    class MySet<E> implements Set<E> {

        @Override
        public int size() {

            return 0;
        }

        @Override
        public boolean isEmpty() {

            return false;
        }

        @Override
        public boolean contains(Object o) {

            return false;
        }

        @Override
        public Iterator<E> iterator() {

            return null;
        }

        @Override
        public Object[] toArray() {

            return null;
        }

        @Override
        public <T> T[] toArray(T[] a) {

            return null;
        }

        @Override
        public boolean add(E e) {

            return false;
        }

        @Override
        public boolean remove(Object o) {

            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {

            return false;
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {

            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {

            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c) {

            return false;
        }

        @Override
        public void clear() {

        }

    }
}
