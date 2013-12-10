package info.archinnov.achilles.embedded;

import java.lang.instrument.ClassDefinition;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.net.URL;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.github.jamm.MemoryMeter;

public class InstrumentationHelper {

	public static String locateJar(Class<?> c) throws ClassNotFoundException {
		final URL location;
		final String classLocation = c.getName().replace('.', '/') + ".class";
		final ClassLoader loader = c.getClassLoader();
		if (loader == null) {
			location = ClassLoader.getSystemResource(classLocation);
		} else {
			location = loader.getResource(classLocation);
		}
		if (location != null) {
			Pattern p = Pattern.compile("^.*:(.*)!.*$");
			Matcher m = p.matcher(location.toString());
			if (m.find())
				return m.group(1);
			else
				throw new ClassNotFoundException("Cannot parse location of '" + location
						+ "'.  Probably not loaded from a Jar");
		} else
			throw new ClassNotFoundException("Cannot find class '" + c.getName() + " using the classloader");
	}

	public static void loadJammAgent() {
		// String nameOfRunningVM =
		// ManagementFactory.getRuntimeMXBean().getName();
		// int p = nameOfRunningVM.indexOf('@');
		// String pid = nameOfRunningVM.substring(0, p);
		//
		// try {
		// String jarFilePath = locateJar(MemoryMeter.class);
		// VirtualMachine vm = VirtualMachine.attach(pid);
		// // vm.loadAgentPath(jarFilePath);
		// vm.loadAgent(jarFilePath);
		// vm.detach();
		// } catch (Exception e) {
		// e.printStackTrace();
		// throw new RuntimeException(e);
		// }

		MemoryMeter.premain("", new Instrumentation() {

			@Override
			public void setNativeMethodPrefix(ClassFileTransformer transformer, String prefix) {
				// TODO Auto-generated method stub

			}

			@Override
			public void retransformClasses(Class<?>... classes) throws UnmodifiableClassException {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean removeTransformer(ClassFileTransformer transformer) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public void redefineClasses(ClassDefinition... definitions) throws ClassNotFoundException,
					UnmodifiableClassException {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean isRetransformClassesSupported() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isRedefineClassesSupported() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isNativeMethodPrefixSupported() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isModifiableClass(Class<?> theClass) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public long getObjectSize(Object objectToSize) {
				// TODO Auto-generated method stub
				return 1L;
			}

			@Override
			public Class[] getInitiatedClasses(ClassLoader loader) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Class[] getAllLoadedClasses() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void appendToSystemClassLoaderSearch(JarFile jarfile) {
				// TODO Auto-generated method stub

			}

			@Override
			public void appendToBootstrapClassLoaderSearch(JarFile jarfile) {
				// TODO Auto-generated method stub

			}

			@Override
			public void addTransformer(ClassFileTransformer transformer, boolean canRetransform) {
				// TODO Auto-generated method stub

			}

			@Override
			public void addTransformer(ClassFileTransformer transformer) {
				// TODO Auto-generated method stub

			}
		});
	}
}
