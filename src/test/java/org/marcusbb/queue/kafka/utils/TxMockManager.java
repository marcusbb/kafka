package org.marcusbb.queue.kafka.utils;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

public class TxMockManager implements TransactionManager{

	public int txStatus = Status.STATUS_ACTIVE;
	public List<Synchronization> syncList = new ArrayList<>();
	
	@Override
	public void begin() throws NotSupportedException, SystemException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, SystemException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getStatus() throws SystemException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Transaction getTransaction() throws SystemException {
		return new Transaction(){

			@Override
			public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
					SecurityException, IllegalStateException, SystemException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean enlistResource(XAResource xaRes)
					throws RollbackException, IllegalStateException, SystemException {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public int getStatus() throws SystemException {
				return txStatus;
			}

			@Override
			public void registerSynchronization(Synchronization sync)
					throws RollbackException, IllegalStateException, SystemException {
				syncList.add(sync);
				
			}

			@Override
			public void rollback() throws IllegalStateException, SystemException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void setRollbackOnly() throws IllegalStateException, SystemException {
				// TODO Auto-generated method stub
				
			}
			
		};
	}

	@Override
	public void resume(Transaction tobj) throws InvalidTransactionException, IllegalStateException, SystemException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setRollbackOnly() throws IllegalStateException, SystemException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setTransactionTimeout(int seconds) throws SystemException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Transaction suspend() throws SystemException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void triggerBefore() {
		for (Synchronization syncItem: syncList) {
			syncItem.beforeCompletion();
		}
	}
	public void triggerAfter() {
		for (Synchronization syncItem: syncList) {
			syncItem.afterCompletion(txStatus);
		}
	}

}
