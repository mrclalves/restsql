/* Copyright (c) restSQL Project Contributors. Licensed under MIT. */
package org.restsql.service;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

import org.apache.shiro.mgt.SubjectFactory;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.SubjectContext;
import org.restsql.core.Config;
import org.restsql.service.monitoring.MonitoringFactory;
import org.secnod.shiro.jersey.AuthInjectableProvider;
import org.secnod.shiro.jersey.ShiroResourceFilterFactory;
import org.secnod.shiro.jersey.SubjectInjectableProvider;

/**
 * Identifies JAX-RS resources through code, since the declarative Jersey scanner does not work with JBoss AS. 
 * 
 * @author Mark Sawers
 */
public class RestSqlApplication extends Application {
	/** Initializes metrics. */
	public RestSqlApplication() {
	}

	/** Configures all the resource classes for the app. */
	@Override
	public Set<Class<?>> getClasses() {
		Set<Class<?>> classes = new HashSet<Class<?>>();
		classes.add(ConfResource.class);
		classes.add(LogResource.class);
		classes.add(ResResource.class);
		classes.add(WadlResource.class);
		classes.add(ToolsResource.class);
		
		return classes;
	}

	@Override
	public Set<Object> getSingletons() {
		try {
			Set<Object> singletons = MonitoringFactory.getMonitoringManager().getApplicationSingletons();
			singletons.add(new ShiroResourceFilterFactory());
			singletons.add(new SubjectFactory() {
				
				@Override
				public Subject createSubject(SubjectContext arg0) {
					// TODO Auto-generated method stub
					return null;
				}
			});
			
			return MonitoringFactory.getMonitoringManager().getApplicationSingletons();
		} catch (Throwable throwable) {
			Config.logger.error(String.format(
					"Error getting application singletons from monitoring manager [%s]",
					MonitoringFactory.getMonitoringManagerClass()), throwable);
			return new HashSet<Object>(0);
		}
	}
}