package fr.doan.achilles.metadata;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("rawtypes")
public class SetPropertyMeta<V extends Serializable> extends SimplePropertyMeta<V> {

    private Class<? extends Set> setClass;

    @SuppressWarnings("unchecked")
    public Set<V> newSetInstance() {
        Set<V> set;
        try {
            set = this.setClass.newInstance();
        } catch (InstantiationException e) {
            set = new HashSet<V>();
        } catch (IllegalAccessException e) {
            set = new HashSet<V>();
        }

        return set;
    }

    public void setSetClass(Class<? extends Set> setClass) {
        this.setClass = setClass;
    }

    @Override
    public PropertyType propertyType() {
        return PropertyType.SET;
    }

}
