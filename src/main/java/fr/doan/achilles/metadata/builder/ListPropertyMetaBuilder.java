package fr.doan.achilles.metadata.builder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.validation.Validator;

@SuppressWarnings("rawtypes")
public class ListPropertyMetaBuilder<V extends Serializable> extends SimplePropertyMetaBuilder<V> {

    private Class<? extends List> listClass;

    public static <V extends Serializable> ListPropertyMetaBuilder<V> listPropertyMetaBuilder(Class<V> valueClass) {
        return new ListPropertyMetaBuilder<V>(valueClass);
    }

    public ListPropertyMetaBuilder(Class<V> valueClass) {
        super(valueClass);
    }

    @Override
    public ListPropertyMeta<V> build() {

        Validator.validateNotNull(listClass, "listClass");
        ListPropertyMeta<V> meta = new ListPropertyMeta<V>();
        super.build(meta);
        if (listClass == List.class) {
            meta.setListClass(ArrayList.class);
        } else {
            meta.setListClass(listClass);
        }
        return meta;
    }

    public ListPropertyMetaBuilder<V> listClass(Class<? extends List> listClass) {
        this.listClass = listClass;
        return this;
    }
}
