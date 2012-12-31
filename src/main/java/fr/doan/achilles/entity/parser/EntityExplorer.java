package fr.doan.achilles.entity.parser;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class EntityExplorer {

    public List<Class<?>> discoverEntities(List<String> packageNames) throws ClassNotFoundException, IOException {
        List<Class<?>> candidates = new ArrayList<Class<?>>();
        for (String packageName : packageNames) {
            candidates.addAll(this.listCandidateClassesFromPackage(packageName, javax.persistence.Table.class));
        }
        return candidates;
    }

    public List<Class<?>> listCandidateClassesFromPackage(String packageName,
            Class<? extends Annotation> annotationClass) throws ClassNotFoundException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;
        String path = packageName.replace('.', File.separatorChar);
        Enumeration<URL> resources = classLoader.getResources(path);
        List<String> dirs = new ArrayList<String>();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(URLDecoder.decode(resource.getFile(), "UTF-8"));
        }
        TreeSet<String> classes = new TreeSet<String>();
        for (String directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        ArrayList<Class<?>> classList = new ArrayList<Class<?>>();
        for (String className : classes) {
            Class<?> clazz = Class.forName(className);
            if (clazz.isAnnotationPresent(annotationClass)) {
                classList.add(clazz);
            }
        }
        return classList;
    }

    private static TreeSet<String> findClasses(String path, String packageName) throws MalformedURLException,
            IOException {
        TreeSet<String> classes = new TreeSet<String>();
        if (path.startsWith("file:") && path.contains("!")) {
            String[] split = path.split("!");
            URL jar = new URL(split[0]);
            ZipInputStream zip = new ZipInputStream(jar.openStream());
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                if (entry.getName().endsWith(".class")) {
                    String className = entry.getName().replaceAll("[$].*", "").replaceAll("[.]class", "")
                            .replace(File.separatorChar, '.');
                    if (className.startsWith(packageName)) {
                        classes.add(className);
                    }
                }
            }
        }
        File dir = new File(path);
        if (!dir.exists()) {
            return classes;
        }
        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file.getAbsolutePath(), packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                String className = packageName + '.' + file.getName().substring(0, file.getName().length() - 6);
                classes.add(className);
            }
        }
        return classes;
    }

}
