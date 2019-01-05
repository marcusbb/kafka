package org.marcusbb.queue.kafka.utils;

import java.util.HashMap;
import java.util.Hashtable;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

public class MockCtxFactory implements InitialContextFactory {

	static Context mockContext = new MockContext();
	@Override
	public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
		return mockContext;
	}
	
	public static class MockContext implements Context{

		HashMap<String, Object> map = new HashMap<>();
		@Override
		public Object lookup(Name name) throws NamingException {
			return map.get(name.toString());
		}

		@Override
		public Object lookup(String name) throws NamingException {
			return map.get(name);
		}

		@Override
		public void bind(Name name, Object obj) throws NamingException {
			map.put(name.toString(), obj);
			
		}

		@Override
		public void bind(String name, Object obj) throws NamingException {
			map.put(name, obj);
			
		}

		@Override
		public void rebind(Name name, Object obj) throws NamingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void rebind(String name, Object obj) throws NamingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void unbind(Name name) throws NamingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void unbind(String name) throws NamingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void rename(Name oldName, Name newName) throws NamingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void rename(String oldName, String newName) throws NamingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void destroySubcontext(Name name) throws NamingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void destroySubcontext(String name) throws NamingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Context createSubcontext(Name name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Context createSubcontext(String name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object lookupLink(Name name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object lookupLink(String name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NameParser getNameParser(Name name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NameParser getNameParser(String name) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Name composeName(Name name, Name prefix) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String composeName(String name, String prefix) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object addToEnvironment(String propName, Object propVal) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object removeFromEnvironment(String propName) throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Hashtable<?, ?> getEnvironment() throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void close() throws NamingException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public String getNameInNamespace() throws NamingException {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
