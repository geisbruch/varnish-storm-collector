package com.ml.storm.varnishlog.dto;

import java.lang.instrument.ClassDefinition;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.util.jar.JarFile;

public class InstrumentationFake implements Instrumentation {

	@Override
	public void addTransformer(ClassFileTransformer transformer) {

	}

	@Override
	public void addTransformer(ClassFileTransformer transformer,
			boolean canRetransform) {

	}

	@Override
	public void appendToBootstrapClassLoaderSearch(JarFile jarfile) {

	}

	@Override
	public void appendToSystemClassLoaderSearch(JarFile jarfile) {
	}

	@Override
	public Class[] getAllLoadedClasses() {
		return null;
	}

	@Override
	public Class[] getInitiatedClasses(ClassLoader loader) {
		return null;
	}

	@Override
	public long getObjectSize(Object objectToSize) {
		return 0;
	}

	@Override
	public boolean isModifiableClass(Class<?> theClass) {
		return false;
	}

	@Override
	public boolean isNativeMethodPrefixSupported() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isRedefineClassesSupported() {
		return false;
	}

	@Override
	public boolean isRetransformClassesSupported() {
		return false;
	}

	@Override
	public void redefineClasses(ClassDefinition... definitions)
			throws ClassNotFoundException, UnmodifiableClassException {
	}

	@Override
	public boolean removeTransformer(ClassFileTransformer transformer) {
		return false;
	}

	@Override
	public void retransformClasses(Class<?>... classes)
			throws UnmodifiableClassException {

	}

	@Override
	public void setNativeMethodPrefix(ClassFileTransformer transformer,
			String prefix) {
	}

}
