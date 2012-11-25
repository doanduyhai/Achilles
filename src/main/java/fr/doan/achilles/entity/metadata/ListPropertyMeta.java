package fr.doan.achilles.entity.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("rawtypes")
public class ListPropertyMeta<V extends Serializable> extends SimplePropertyMeta<V> {

    private Class<? extends List> listClass;

    @SuppressWarnings("unchecked")
    public List<V> newListInstance() {
        List<V> list;
        try {
            list = this.listClass.newInstance();
        } catch (InstantiationException e) {
            list = new ArrayList<V>();
        } catch (IllegalAccessException e) {
            list = new ArrayList<V>();
        }
        return list;
    }

    public void setListClass(Class<? extends List> listClass) {
        this.listClass = listClass;
    }

    @Override
    public PropertyType propertyType() {
        return PropertyType.LIST;
    }

}
