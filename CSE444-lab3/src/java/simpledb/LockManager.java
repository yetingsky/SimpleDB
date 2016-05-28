package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockManager {
    private static final int SHARED_LOCK = 0;
    private static final int EXCLUSIVE_LOCK = 1;
    private Map<PageId, Pair> locks;
    
    public LockManager() {
        locks = new HashMap<>();
    }
    
    
    public static class Pair {
        public int lockType;
        public Set<TransactionId> tids;
        
        public Pair(TransactionId tid, int lockType) {
            this.lockType = lockType;
            this.tids = new HashSet<>();
            tids.add(tid);
        }
        
        public Set<TransactionId> getTransactions() {
            return tids;
        }
    }

    public void getLock(TransactionId tid, PageId pid, Permissions perm) {        
        if (perm ==Permissions.READ_WRITE) {
            getExclusiveLock(tid, pid);
        } else {
            getSharedLock(tid, pid);
        }
    }
    
    private void getExclusiveLock(TransactionId tid, PageId pid) {
        while (!(locks.containsKey(pid) && 
                locks.get(pid).lockType == EXCLUSIVE_LOCK &&
                locks.get(pid).getTransactions().size() == 1 &&
                locks.get(pid).getTransactions().contains(tid))) {
            synchronized (pid) {
                if (!locks.containsKey(pid)) {
                    locks.put(pid, new Pair(tid, EXCLUSIVE_LOCK));
                } else if (locks.get(pid).getTransactions().size() == 1 &&
                           locks.get(pid).getTransactions().contains(tid)) {
                    locks.get(pid).lockType = EXCLUSIVE_LOCK;  // upgrade to exclusive lock
                }
            }
        }
    }
    
    private void getSharedLock(TransactionId tid, PageId pid) {
        while (!(locks.containsKey(pid) &&
                locks.get(pid).getTransactions().contains(tid))) {
           synchronized (pid) {
               if (!locks.containsKey(pid)) {
                   locks.put(pid, new Pair(tid, SHARED_LOCK));
               } else if (locks.get(pid).lockType == SHARED_LOCK){
                   locks.get(pid).getTransactions().add(tid);
               }
           }
       }
    }
    
    
    public void releaseLock(TransactionId tid, PageId pid) {
        if (locks.containsKey(pid)) {
            Set<TransactionId> tids = locks.get(pid).tids;
            tids.remove(tid);
            if (tids.isEmpty()) {
                locks.remove(pid);
            }
        }
    }
    
    public synchronized void releaseAllLock(TransactionId tid) {
        
    }
    
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        return locks.containsKey(pid);
    }
}
