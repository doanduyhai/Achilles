package fr.doan.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import org.junit.Test;

import fr.doan.achilles.entity.metadata.ListPropertyMeta;

public class ListPropertyMetaTest {

    @Test
    public void should_exception_when_cannot_instanciate() throws Exception {
        ListPropertyMeta<String> listMeta = new ListPropertyMeta<String>();
        listMeta.setListClass(MyList.class);

        List<String> list = listMeta.newListInstance();

        assertThat(list).isInstanceOf(ArrayList.class);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    class MyList<E> implements List<E> {

        private MyList() {
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
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Iterator iterator() {
            return null;
        }

        @Override
        public Object[] toArray() {
            return null;
        }

        @Override
        public Object[] toArray(Object[] a) {
            return null;
        }

        @Override
        public boolean add(Object e) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection c) {
            return false;
        }

        @Override
        public boolean addAll(Collection c) {
            return false;
        }

        @Override
        public boolean addAll(int index, Collection c) {
            return false;
        }

        @Override
        public boolean removeAll(Collection c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection c) {
            return false;
        }

        @Override
        public void clear() {

        }

        @Override
        public E get(int index) {
            return null;
        }

        @Override
        public E set(int index, Object element) {
            return null;
        }

        @Override
        public void add(int index, Object element) {

        }

        @Override
        public E remove(int index) {
            return null;
        }

        @Override
        public int indexOf(Object o) {
            return 0;
        }

        @Override
        public int lastIndexOf(Object o) {
            return 0;
        }

        @Override
        public ListIterator listIterator() {
            return null;
        }

        @Override
        public ListIterator listIterator(int index) {
            return null;
        }

        @Override
        public List subList(int fromIndex, int toIndex) {
            return null;
        }

    }
}
