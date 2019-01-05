package org.marcusbb.queue.kafka.producer;

import java.util.LinkedList;
import java.util.Queue;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JTAProducerSync<T> implements Synchronization,Producer<T> {

	private static Logger logger = LoggerFactory.getLogger(JTAProducerSync.class);
	
	private static final ThreadLocal<Queue<MessageWrapper>> transientQueue = new ThreadLocal<Queue<MessageWrapper>>(){

		@Override
		protected Queue<MessageWrapper> initialValue() {
			return new LinkedList<>();
		}
		
	};
	private static final ThreadLocal<Transaction> curTx = new ThreadLocal<>();
			
	public static String DEF_TX_MANAGER = "java:comp/TransactionManager";
	
	public static String DEF_TX_REGISTRY = 	"java:comp/TransactionSynchronizationRegistry";
			
	boolean useRegistry = false;
		
	volatile boolean msgDeliveryFailure = false;

	protected String topic;
	protected TransactionManager txManager;
	//Alternative method of enlisting synchronization
	private TransactionSynchronizationRegistry txRegistry;
	private static final ThreadLocal<Object> curTxObject = new ThreadLocal<>();

	public JTAProducerSync() throws NamingException {
		InitialContext ctx = new InitialContext();
		String ln = DEF_TX_REGISTRY;
		// To use the registry method you have to set this system property
		if (System.getProperty("org.marcusbb.queue.jta.sync") != null)
			ln = System.getProperty("org.marcusbb.queue.jta.sync");
		Object txSync = ctx.lookup(ln);
		if (txSync instanceof TransactionManager) {
			this.txManager = (TransactionManager) txSync;
		} else {
			this.txRegistry = (TransactionSynchronizationRegistry) txSync;
			useRegistry = true;
		}
	}	
	
	public JTAProducerSync(TransactionManager txManager) {
		
		this.txManager = txManager;
		
	}
	public JTAProducerSync(TransactionSynchronizationRegistry registry) {
		this.txRegistry = registry;
		useRegistry = true;
	}
	
	@Override
	public void afterCompletion(int status) {

		//Nothing to do here

        if (Status.STATUS_COMMITTED != status) {
			logger.warn("Uncommitted completion of produce to topic {}; transaction {} with status {}",
					topic, useRegistry ? curTxObject.get() : curTx.get(), status);
		}
	}
	@Override
	public void beforeCompletion() {
			
		if (!useRegistry && curTx.get() == null && txManager == null)
			throw new IllegalStateException("Transaction was not registered");
		
		int txStatus = Status.STATUS_UNKNOWN;
		
		
		try {
			if (!useRegistry && curTx.get() != null)
				txStatus = curTx.get().getStatus();
			else if (txRegistry != null)
				txStatus = txRegistry.getTransactionStatus();
			else 
				throw new IllegalStateException("Transaction Manager or Registry not properly initialized");
			
			onComplete(txStatus);
		} catch (SystemException e) {
			throw new RuntimeException("Fatal system exception",e);
		}
	}
	
	private void onComplete(int status) {
		try {

			if (status == Status.STATUS_COMMITTING || status == Status.STATUS_ACTIVE || status == Status.STATUS_COMMITTED) {
				//as long as this throws here we should be able to roll back the transaction
				logger.info("Sending msg to Kafka topic {}; transaction {} with status {} being executed...",
						topic, useRegistry ? curTxObject.get() : curTx.get(),status);
				flush(transientQueue.get());
				logger.info("sent msg to Kafka topic {}; transaction {} implicitly committed.",
						topic, useRegistry ? curTxObject.get() : curTx.get());

			} else {
				logger.warn("Topic {}; transaction {} with status {} NOT committed", topic, useRegistry ? curTxObject.get() : curTx.get(),status);
			}
		} finally {
			//possible to have multiple after completion statuses? implementation will need to change
			initialize();
		}
	}
	 
	@Override
	public void rollback() {
		transientQueue.set(new LinkedList<MessageWrapper>() );
	}
	
	
	public abstract void flush(Queue<MessageWrapper> messages);
	
	
	@Override
	public void flush() {
		flush(transientQueue.get());
		initialize();
	}
	private void initialize() {
		transientQueue.set(new LinkedList<MessageWrapper>() );
		curTx.set(null);
		curTxObject.set(null);
	}

	@Override
	public void add(String topic, T message) {
		add(topic,message,null);
	}

	@Override
	public void add(String topic, T message, Object partitionKey) {
		this.topic = topic;
		enlist();
		transientQueue.get().add(new MessageWrapper<>(message, partitionKey));
	}

	private void enlist() {
		
		if (useRegistry && txRegistry.getTransactionKey() != null && !txRegistry.getTransactionKey().equals(curTxObject.get())) {
			curTxObject.set(txRegistry.getTransactionKey());
			txRegistry.registerInterposedSynchronization(this);
		}
		else if (!useRegistry && curTx.get() == null) {
			try {
				if (txManager.getTransaction().getStatus() != Status.STATUS_ACTIVE)
					throw new IllegalStateException("The transaction is not alive.");
			}catch (SystemException e) {
				throw new RuntimeException("Fatal system Exception",e);
			}
			try {
				curTx.set(txManager.getTransaction());
				curTx.get().registerSynchronization(this);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} 
		}
	}

	protected final static class MessageWrapper<T> {
		T payload;
		Object partitionKey;

		MessageWrapper(T payload, Object partitionKey) {
			super();
			this.payload = payload;
			this.partitionKey = partitionKey;
		}
		public T getPayload() {
			return payload;
		}
		public Object getPartitionKey() {
			return partitionKey;
		}
		
	}

}
