package fr.doan.achilles.proxy.interceptor;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import fr.doan.achilles.validation.Validator;

public class NoOpInterceptor implements MethodInterceptor, AchillesInterceptor
{
	private Object realObject;

	public NoOpInterceptor(Object realObject) {
		Validator.validateNotNull(realObject, "real object to proxy");
		this.realObject = realObject;
	}

	@Override
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable
	{
		return proxy.invokeSuper(realObject, args);
	}

	@Override
	public Object getRealObject()
	{
		return realObject;
	}

}
