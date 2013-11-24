package info.archinnov.achilles.entity.manager;

import java.util.List;

import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.EventInterceptor;
import info.archinnov.achilles.type.Options;

public class Toto implements EventInterceptor<Options> {

	@Override
	public List<Event> events() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Options onEvent(Options entity) {
		return null;
	}

}
