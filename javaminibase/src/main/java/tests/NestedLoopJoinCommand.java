package tests;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import columnar.*;
import bitmap.*;
import java.io.*;
import java.util.Arrays;
import diskmgr.pcounter;

public class NestedLoopJoinCommand {
    /*
     * Command line invocation of:
     * nlj COLUMNDBNAME OUTERFILE INNERFILE OUTERCONST INNERCONST JOINCONST OUTERACCTYPE [TARGETCOLUMNS] NUMBUF
     *
     * COLUMNDBNAME is the DB name
     * OUTERFILE the columnarfile of the outer relation of the join
     * INNERFILE the columnarfile of the inner relation of the join
     * OUTERCONST is the selection constraints that apply to the outer relation
     * INNERCONST is the selection constraints that apply to the inner relation
     * JOINCONST is the join and selection constraints that apply to combined tuples (general constraints)
     * OUTERACCTYPE is either FILESCAN, COLUMNSCAN, BTREE, or BITMAP
     * TARGETCOLUMNS is the column(s) to project from the resulting tuples
     * NUMBUF is an integer (MiniBase will use at most NUMBUF buffer pages to perform the join)
     */

    public static void run( String[] args ) throws HFDiskMgrException, InvalidTupleSizeException, HFException, InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException, IOException, AddFileEntryException, GetFileEntryException, ConstructPageException {
        pcounter.initialize(); // Initializes read & write counters to 0

        // TODO: parse input and implement nested loop join

        try {
            SystemDefs.JavabaseBM.resetAllPins();
            SystemDefs.JavabaseBM.flushAllPages();
        } catch (Exception ex){
            System.out.println("could not flush the pages");
            ex.printStackTrace();
        }
        System.out.println("Index test finished!\n");
        System.out.println("Disk read count: " + pcounter.rcounter);
        System.out.println("Disk write count: " + pcounter.wcounter);
    }
}
