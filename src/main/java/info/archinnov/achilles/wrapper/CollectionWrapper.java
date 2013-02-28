package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.wrapper.builder.IteratorWrapperBuilder.builder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * CollectionWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CollectionWrapper<V> extends AbstractWrapper<Void, V> implements Collection<V> {
    protected Collection<V> target;

    public CollectionWrapper(Collection<V> target) {
        if (target == null) {
            this.target = new ArrayList<V>();
        } else {
            this.target = target;
        }
    }

    @Override
    public boolean add(V arg0) {

        boolean result = target.add(helper.unproxy(arg0));
        this.markDirty();
        return result;
    }

    @Override
    public boolean addAll(Collection<? extends V> arg0) {

        boolean result = target.addAll(helper.unproxy(arg0));
        if (result) {
            this.markDirty();
        }
        return result;
    }

    @Override
    public void clear() {
        if (this.target.size() > 0) {
            this.markDirty();
        }
        this.target.clear();
    }

    @Override
    public boolean contains(Object arg0) {
        return this.target.contains(helper.unproxy(arg0));
    }

    @Override
    public boolean containsAll(Collection<?> arg0) {
        return this.target.containsAll(helper.unproxy(arg0));
    }

    @Override
    public boolean isEmpty() {
        return this.target.isEmpty();
    }

    @Override
    public Iterator<V> iterator() {
        return builder(this.target.iterator()) //
                .dirtyMap(dirtyMap) //
                .setter(setter) //
                .propertyMeta(propertyMeta) //
                .helper(helper) //
                .build();
    }

    @Override
    public boolean remove(Object arg0) {
        boolean result = this.target.remove(helper.unproxy(arg0));
        if (result) {
            this.markDirty();
        }
        return result;
    }

    @Override
    public boolean removeAll(Collection<?> arg0) {
        boolean result = this.target.removeAll(helper.unproxy(arg0));
        if (result) {
            this.markDirty();
        }
        return result;
    }

    @Override
    public boolean retainAll(Collection<?> arg0) {
        boolean result = this.target.retainAll(helper.unproxy(arg0));
        if (result) {
            this.markDirty();
        }
        return result;
    }

    @Override
    public int size() {
        return this.target.size();
    }

    @Override
    public Object[] toArray() {

        if (isJoin()) {
            Object[] array = new Object[this.target.size()];
            int i = 0;
            for (V joinEntity : this.target) {
                array[i] = helper.buildProxy(joinEntity, joinMeta());
                i++;
            }

            return array;
        } else {

            return this.target.toArray();
        }
    }

    @Override
    public <T> T[] toArray(T[] arg0) {
        if (isJoin()) {
            T[] array = this.target.toArray(arg0);

            for (int i = 0; i < array.length; i++) {
                array[i] = helper.buildProxy(array[i], joinMeta());
            }
            return array;
        } else {

            return this.target.toArray(arg0);
        }
    }

    public Collection<V> getTarget() {
        return this.target;
    }
}
