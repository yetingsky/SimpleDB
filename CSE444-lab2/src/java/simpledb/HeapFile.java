package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    
    private File f;
    private TupleDesc td;
    private int tableid;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        if (tableid == 0) {
            tableid = f.getAbsoluteFile().hashCode();
        }
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile rf = new RandomAccessFile(f, "r");
            byte[] data = new byte[BufferPool.PAGE_SIZE];
            rf.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
            rf.read(data);
            rf.close();
            return new HeapPage((HeapPageId)pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
//        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
//        // some code goes here
//        // not necessary for lab1
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(page.getId().pageNumber() * BufferPool.PAGE_SIZE);
        rf.write(page.getPageData());
        rf.close();    
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> result = new ArrayList<>();
        BufferPool bfpool = Database.getBufferPool();
        int page_num = numPages();
        for (int i = 0; i < page_num; i++) {
            // must read page from buffer pool
            HeapPage page = (HeapPage) bfpool.getPage(tid, new HeapPageId(getId(), i), null);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                result.add(page);
                return result;
            }
        }
        // all page in this file is full, need create a new empty page
        writePage(new HeapPage(new HeapPageId(getId(), page_num), HeapPage.createEmptyPageData()));
        // again, read this page by buffer pool
        HeapPage page = (HeapPage) bfpool.getPage(tid, new HeapPageId(getId(), page_num), null);
        page.insertTuple(t);
        result.add(page);
        return result;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> result = new ArrayList<>();
        // must read page from buffer pool
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null);
        page.deleteTuple(t);
        result.add(page);
        return result;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(getId(), numPages(), tid);
    }

}

