package org.marcusbb.queue.kafka;

import javax.naming.InitialContext;

import org.junit.Assert;
import org.junit.Test;
import org.marcusbb.queue.kafka.utils.TxMockManager;

public class JNDITest {

	@Test
	public void fail() throws Exception {
		System.setProperty("java.naming.factory.initial", "org.marcusbb.queue.kafka.utils.MockCtxFactory");
		InitialContext ctx = new InitialContext();
		ctx.bind("java:comp/TransctionManager", new TxMockManager());
		
		ctx = new InitialContext();
		Assert.assertNotNull(ctx.lookup("java:comp/TransctionManager"));
	}
}
