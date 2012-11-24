package fr.doan.achilles.enhancer;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class TestMethodInterceptor implements MethodInterceptor
{

	private Object realObj;

	public TestMethodInterceptor(Object realObj) {
		this.realObj = realObj;
	}

	@Override
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable
	{
		System.out.println("Method " + method.getName() + " called ");
		return proxy.invokeSuper(obj, args);
	}

}
