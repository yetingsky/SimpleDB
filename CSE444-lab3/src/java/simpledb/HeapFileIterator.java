package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    
    private int tableId;
    private int pgNo;  // -1 means close, greater than 0 means open
    private int numPages;
    private TransactionId tid;
    private Permissions perm;
    private Iterator<Tuple> tuples;
    
    public HeapFileIterator(int tableId, int numPages, TransactionId tid, Permissions perm) {
        this.tableId = tableId;
        this.numPages = numPages;
        this.tid = tid;
        this.pgNo = -1;
        this.tuples = null;
        this.perm = perm;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        if (!isOpen()) {
            pgNo = 0;
            tuples = getTuples();
        } else {
            throw new DbException("this iterator already open");
        }
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!isOpen()) {
            return false;
        }
        
        if (tuples != null && tuples.hasNext()) {
            return true;
        } else if (pgNo == numPages) {
            return false;
        } else {
            tuples = getTuples();
            return hasNext();
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return tuples.next();
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        pgNo = -1;
        tuples = null;
    }
    
    private Iterator<Tuple> getTuples() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (pgNo == numPages)
            return null;
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo), perm);
        pgNo++;
        return page.iterator();
    }
    
    private boolean isOpen() {
        return pgNo != -1;
    }
}
