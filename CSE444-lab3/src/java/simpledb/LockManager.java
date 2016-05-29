package simpledb;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.mina.util.ConcurrentHashSet;

public class LockManager {
    private static final int SHARED_LOCK = 0;
    private static final int EXCLUSIVE_LOCK = 1;
    private Map<PageId, Lock> locks;
    private Map<PageId, Object> lockObject;
    private Map<TransactionId, Collection<TransactionId>> dependencyGraph;
    private Map<TransactionId, Collection<PageId>> locksByTransaction;
    
    public LockManager() {
        locks = new HashMap<>();
        lockObject = new ConcurrentHashMap<>();
        dependencyGraph = new ConcurrentHashMap<>();
        locksByTransaction = new ConcurrentHashMap<>();
    }
    
    public static class Lock {
        public int lockType;
        public Set<TransactionId> tids;
        
        public Lock(TransactionId tid, int lockType) {
            this.lockType = lockType;
            this.tids = new ConcurrentHashSet<>();
            tids.add(tid);
        }
        
        public Set<TransactionId> getTransactions() {
            return tids;
        }
    }

    public void accquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException { 
        if (perm ==Permissions.READ_WRITE) {
            accquireExclusiveLock(tid, pid);
        } else {
            accquireSharedLock(tid, pid);
        }
        
        addLockInTransaction(tid, pid);
    }
    
    public void releaseLock(TransactionId tid, PageId pid) {
        // won't remove pid in locksByTransaction
        Object lock = getLock(pid);
        synchronized (lock) {
            if (locks.containsKey(pid)) {
                Set<TransactionId> tids = locks.get(pid).tids;
                tids.remove(tid);
                if (tids.isEmpty()) {
                    locks.remove(pid);
                }
            }
        }

        if (locksByTransaction.containsKey(tid)) {
            Set<PageId> pids = (ConcurrentHashSet<PageId>) locksByTransaction.get(tid);
            pids.remove(pid);
            if (pids.isEmpty()) {
                locksByTransaction.remove(tid);
            }
        }
    }
    
    public void releaseAllLock(TransactionId tid) {
        if (!locksByTransaction.containsKey(tid)) {
            return;
        }
        
        Set<PageId> pids = (ConcurrentHashSet<PageId>) locksByTransaction.get(tid);
        Set<PageId> pidsCopy = new ConcurrentHashSet<>();
        for (PageId pid: pids) {
            pidsCopy.add(pid);
        } 
        
        for (PageId pid: pidsCopy) {
            releaseLock(tid, pid);
        }
    }
    
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return locks.containsKey(pid) && locks.get(pid).getTransactions().contains(tid);
    }
    
    private void accquireExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        // must use same lock Object to synchronize
        // because PageId Objects may equal but not same object
        // so use PageId object can not correctly synchronize
        // use a hashmap map equal PageId to same object(lock)
        Object lock = getLock(pid);
        while (!hasExclusiveLock(tid, pid)) {
            synchronized (lock) {
                if (!locks.containsKey(pid)) {
                    locks.put(pid, new Lock(tid, EXCLUSIVE_LOCK));
                    removeDependencies(tid);
                } else if (locks.get(pid).getTransactions().size() == 1 &&
                           locks.get(pid).getTransactions().contains(tid)) {
                    locks.get(pid).lockType = EXCLUSIVE_LOCK;  // upgrade to exclusive lock
                    removeDependencies(tid);
                } else {
                    addDependencies(tid, locks.get(pid).getTransactions());
                }
            }
        }
    }
    
    private void accquireSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Object lock = getLock(pid);
        while (!hasSharedLock(tid, pid)) {
           synchronized (lock) {
               if (!locks.containsKey(pid)) {
                   locks.put(pid, new Lock(tid, SHARED_LOCK));
                   removeDependencies(tid);
               } else if (locks.get(pid).lockType == SHARED_LOCK){
                   locks.get(pid).getTransactions().add(tid);
                   removeDependencies(tid);
               } else {
                   addDependencies(tid, locks.get(pid).getTransactions());
               }
           }
       }
    }
    
    private boolean hasSharedLock(TransactionId tid, PageId pid) {
        Object lock = getLock(pid);
        synchronized(lock) {
            return locks.containsKey(pid) && locks.get(pid).getTransactions().contains(tid);
        }
    }
    
    private boolean hasExclusiveLock(TransactionId tid, PageId pid) {
        Object lock = getLock(pid);
        synchronized(lock) {
            return locks.containsKey(pid) && 
                   locks.get(pid).lockType == EXCLUSIVE_LOCK &&
                   locks.get(pid).getTransactions().size() == 1 &&
                   locks.get(pid).getTransactions().contains(tid);
        }
    }
    
    private Object getLock(PageId pid) {
        if (!lockObject.containsKey(pid)) {
            lockObject.put(pid, new Object());
        }
        return lockObject.get(pid);
    }
    
    private void addDependencies(TransactionId tid, Set<TransactionId> waittids) throws TransactionAbortedException {
        if (!dependencyGraph.containsKey(tid)) {
            dependencyGraph.put(tid, new ConcurrentHashSet<>());
        }
        
        boolean hasAdd = false;
        ConcurrentHashSet<TransactionId> hasWait = (ConcurrentHashSet<TransactionId>) dependencyGraph.get(tid);
        for (TransactionId waittid: waittids) {
            hasWait.add(waittid);
            hasAdd = true;
        }
        
        if (hasAdd) {
            abortIfDeadLock(tid);
        }
    }
    
    private void removeDependencies(TransactionId tid) {
        if (dependencyGraph.containsKey(tid)) {
            dependencyGraph.remove(tid);
        }
    }
    
    private void addLockInTransaction(TransactionId tid, PageId pid) {
        if (!locksByTransaction.containsKey(tid)) {
            locksByTransaction.put(tid, new ConcurrentHashSet<>());
        }
        locksByTransaction.get(tid).add(pid);
    }
    
    /**
     * 
     * @param tid start point
     * if has a cycle, must start from this tid
     */
    private void abortIfDeadLock(TransactionId tid) throws TransactionAbortedException {
        if (hasDeadLock(tid, new Stack<>())) {
            throw new TransactionAbortedException();
        }
    }
    
    private boolean hasDeadLock(TransactionId tid, Stack<TransactionId> stack) {
        if (tid == null) {
            return false;
        }
        if (stack.contains(tid)) {
            return true;
        }
        stack.push(tid);
        if (dependencyGraph.containsKey(tid)) {
            for (TransactionId waittid: dependencyGraph.get(tid)) {
                if (hasDeadLock(waittid, stack)) {
                    return true;
                }
            }
        }
        stack.pop();
        return false;
    }
    
}
