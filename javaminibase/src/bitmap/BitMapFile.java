package bitmap;

import btree.*;
import columnar.Columnarfile;
import columnar.valueClass;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

import java.io.IOException;

/**
 * Create a class called BitMapFile with the following specifications (see BTreeFile for analogy):
 */
public class BitMapFile extends IndexFile implements GlobalConst{

    private String dbname;
    private BitMapHeaderPage headerPage;
    private PageId headerPageId;

    /**
     * BitMapFile class; an index file with given filename should already exist, then this opens it.
     * @param filename the Bit Map File tree file name. Input parameter.
     * @throws GetFileEntryException
     */
    public BitMapFile(java.lang.String filename) throws GetFileEntryException {
        headerPageId=get_file_entry(filename);
        headerPage= new  BitMapHeaderPage(headerPageId);
        dbname = new String(filename);
    }

    /**
     * BitMapFile class; an index file with given filename should not already exist; this creates the BitMap file
     * from scratch.
     * @param filename
     * @param columnfile
     * @param ColumnNo
     * @param value
     */
    public BitMapFile(java.lang.String filename, Columnarfile columnfile, int ColumnNo, valueClass value){
        /*
            todo
        * */
    }

    void close(){
        //    Close the BitMap file.
        //todo
    }

    void destroyBitMapFile(){
        //todo
        //    Destroy the entire BitMap file.
    }

    BitMapHeaderPage getHeaderPage(){
        return null;//todo
        //    Access method to data member.
    }

    boolean Delete(int position){
        return false;// todo
        //    set the entry at the given position to 0.
    }

    boolean Insert(int position){
        return false;//todo
        //    set the entry at the given position to 1.
    }

    private PageId get_file_entry(String filename)
            throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    @Override
    public void insert(KeyClass data, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException {
        //todo maybe, is this required?
    }

    @Override
    public boolean Delete(KeyClass data, RID rid) throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException, KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException, RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException, ConstructPageException, DeleteRecException, IndexSearchException, IOException {
        //todo maybe, is this required?
        return false;
    }
}
