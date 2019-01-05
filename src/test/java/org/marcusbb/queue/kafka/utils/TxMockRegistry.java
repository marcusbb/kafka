package org.marcusbb.queue.kafka.utils;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.Synchronization;
import javax.transaction.TransactionSynchronizationRegistry;

public class TxMockRegistry implements TransactionSynchronizationRegistry {

	public List<Synchronization> syncList = new ArrayList<>();
	
	private Object txObject = new Object();
	
	private int txStatus = 0;
	
	public TxMockRegistry() {
		
	}
	public TxMockRegistry(Object txObject) {
		this.txObject = txObject;
	}
	
	@Override
	public Object getTransactionKey() {
		return txObject;
	}

	@Override
	public void putResource(Object key, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getResource(Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void registerInterposedSynchronization(Synchronization sync) {
		this.syncList.add(sync);
		
	}

	@Override
	public int getTransactionStatus() {
		return txStatus;
	}

	@Override
	public void setRollbackOnly() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean getRollbackOnly() {
		// TODO Auto-generated method stub
		return false;
	}

	public void setTransactionStatus(int status) {
		this.txStatus = status;
	}
	public void resetTxObject() {
		txObject = new Object();
	}
}
