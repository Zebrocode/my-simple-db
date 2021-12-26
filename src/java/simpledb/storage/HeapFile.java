package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * <p>
 * HeapFile --> HeapPages --> Turples
 * 每一个HeapFile对应一张表
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private int tableId;
    private File file;
    //每一个HeapFile对应数据库中一个表,因此这个td也就对应表中的td
    private TupleDesc td;
    private int pageNum;
    //好像不需要缓存,已经有bufferpool了,bufferpool如果缓存没有命中会调用file的readPage接口从文件读

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        int pageSize = BufferPool.getPageSize();
        this.pageNum = (int) (file.length() + pageSize - 1) / pageSize;
        this.tableId = file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.tableId;
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            int pageSize = BufferPool.getPageSize();
            byte[] buff = new byte[pageSize];
            //这里用错了,offset并不是文件中的偏移量,而是buff中拷贝的开始位置
            //            raf.read(buff, offset, buff.length);
            //应该改为下面这种
            raf.seek((long) pid.getPageNumber() * pageSize);
            raf.read(buff);

            HeapPageId hpid = null;
            if (pid instanceof HeapPageId) {
                hpid = (HeapPageId) pid;
            }
            HeapPage heapPage = new HeapPage(hpid, buff);
            return heapPage;
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("file not found~~~");
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("page doesn't exist in this file");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.pageNum;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    public class HeapFileIterator implements DbFileIterator {
        private HeapFile heapFile;
        private TransactionId tid;
        private HeapPage currPage;
        private Iterator<Tuple> tupleIter;
        Boolean open = false;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            PageId pid = new HeapPageId(heapFile.tableId, 0);
            Page page = Database.getBufferPool().getPage(tid, pid, null);
            if (page instanceof HeapPage) {
                this.currPage = (HeapPage) page;//第一个pageId从哪里获取?
            } else {
                throw new DbException("wrong Page type !!!!");
            }
            this.tupleIter = (this.currPage).iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!open) {
                return false;
            }
            if (this.tupleIter.hasNext()) {
                return true;
            }
            //需要翻页了
            HeapPageId id = currPage.getId();
            int nextPageNumber = id.getPageNumber() + 1;
            if (nextPageNumber >= heapFile.pageNum) {
                return false;
            }
            PageId pid = new HeapPageId(tableId, nextPageNumber);
            currPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
            tupleIter = currPage.iterator();
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!open) {
                throw new NoSuchElementException("unOpen iterator!!!");
            }
            return tupleIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!open) {
                throw new DbException("the iterator unOpen can't rewind");
            }
            PageId pid = new HeapPageId(tableId, 0);
            this.currPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
            this.tupleIter = this.currPage.iterator();
        }

        @Override
        public void close() {
            if (open) {
                open = false;
            }
        }
    }

}

