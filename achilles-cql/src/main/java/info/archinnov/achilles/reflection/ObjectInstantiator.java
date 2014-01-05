package info.archinnov.achilles.reflection;

import org.objenesis.ObjenesisStd;

public class ObjectInstantiator {

    /*
    * make property useCache of Objenesis configurable
    */
    private ObjenesisStd objenesisStd = new ObjenesisStd();

    /*
     * Warning !!!
     * Instance init code block and constructor logic
     * will not be executed when creating instance
     * with Objenesis
     */
    public <T> T instantiate(Class<T> entityClass) {
        return objenesisStd.newInstance(entityClass);
    }

}
