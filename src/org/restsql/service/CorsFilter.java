package org.restsql.service;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class CorsFilter implements Filter {

	public CorsFilter() {
	}

	@Override
	public void destroy() {

	}

	@Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException {
	    HttpServletRequest request = (HttpServletRequest) servletRequest;
	    System.out.println("Request request.getMethod() ["+request.getMethod()+"]");

	    HttpServletResponse resp = (HttpServletResponse) servletResponse;
	    resp.addHeader("Access-Control-Allow-Origin","*");
	    resp.addHeader("Access-Control-Allow-Methods","GET,POST");
	    resp.addHeader("Access-Control-Allow-Headers","Origin, X-Requested-With, Content-Type, Accept");
	    resp.addHeader("Access-Control-Allow-Credentials", "false");
	    resp.addHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
	    resp.addHeader("METODO", request.getMethod());
	    // Just ACCEPT and REPLY OK if OPTIONS
	    if ( request.getMethod().equals("OPTIONS") ) {
	        resp.setStatus(HttpServletResponse.SC_OK);
	        return;
	    }
	    chain.doFilter(request, servletResponse);
	}

	@Override
	public void init(FilterConfig arg0) throws ServletException {

	}

}
