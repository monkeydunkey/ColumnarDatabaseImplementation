package bitmap;

import columnar.Columnarfile;
import columnar.valueClass;

/**
 *
 */
public class BitMapFile {

    public BitMapFile(java.lang.String filename){
        /*
        *   BitMapFile class; an index file with given filename
            should already exist, then this opens it.
            todo
        * */
    }

    public BitMapFile(java.lang.String filename, Columnarfile columnfile,
                      int ColumnNo, valueClass value){
        /*
        *   BitMapFile class; an index file with given filename
            should not already exist; this creates the BitMap file
            from scratch.
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
}
