package net.hycube.utils;

public class ClassInstanceLoader {

	public static Object newInstance(String className, Class<?> baseClass) throws ClassInstanceLoadException {
		if (className == null || className.isEmpty()) {
			throw new ClassInstanceLoadException("Error while loading class: Invalid class name specified.");
		}
		Class<?> clazz;
		try {
			clazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new ClassInstanceLoadException("Error while loading class: " + className, e, className);
		}
		return newInstance(clazz, baseClass);
	}
	
	public static Object newInstance(Class<?> clazz, Class<?> baseClass) throws ClassInstanceLoadException {
		if (baseClass == null) {
			throw new ClassInstanceLoadException("Error while loading class: Base class not specified.");
		}
		if (baseClass.isAssignableFrom(clazz)) {
			Object object;
			try {
				object = clazz.newInstance();
			} catch (InstantiationException e) {
				throw new ClassInstanceLoadException("Error while creating an instance of class: " + clazz.getName(), e, clazz);
			} catch (IllegalAccessException e) {
				throw new ClassInstanceLoadException("Error while creating an instance of class: " + clazz.getName(), e, clazz);
			}
			return object;
		}
		else {
			throw new ClassInstanceLoadException("Class " + clazz.getName() + " is not a subclass or does not implement " + baseClass.getName());
		}
	}
	
}
