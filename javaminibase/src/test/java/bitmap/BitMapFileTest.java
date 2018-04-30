package bitmap;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import diskmgr.Page;
import global.*;
import heap.HFBufMgrException;
import heap.HFPage;
import heap.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;

import static org.junit.Assert.*;

public class BitMapFileTest {

    @Before
    public void setup(){
        new File("/Users/james_kieley/Documents/GitHub/CSE510-DBMS-Project/javaminibase/tempdbname").deleteOnExit();
    }

    @Test
    public void name() throws Exception {
        new SystemDefs("tempdbname",100000,100,"Clock");
        LinkedList<Boolean> cursorBuffer = new LinkedList<>();
        cursorBuffer.addAll(Arrays.asList(true,false,false,true,false,false,true,true,true,true,false,true));


        BMPage newBMPage = BitMapFile.getNewBMPage();
        System.out.println("page number: "+newBMPage.getCurPage().pid);
        System.out.println("newBMPage.getSlotCnt(): "+newBMPage.getSlotCnt());
        newBMPage.insertRecord(BitMapFile.toBytes(BitMapFile.toBooleanArray(cursorBuffer)));
        System.out.println(Arrays.toString(newBMPage.data));
        System.out.println("newBMPage.getSlotCnt(): "+newBMPage.getSlotCnt());
        //BM.unpinPage(newBMPage.getCurPage(), true);

        // executed later

        PageId pageno = newBMPage.getCurPage();
        Page page = BM.pinPage(pageno);
        BMPage bmPage = new BMPage(page);
        System.out.println(Arrays.toString(bmPage.data));
        System.out.println("bm page number: "+bmPage.getCurPage().pid);
        System.out.println("newBMPage.getSlotCnt(): "+bmPage.getSlotCnt());
//        if(bmPage.getSlotCnt() == 0 ){
//            throw new RuntimeException("THERE SHOULD BE DATA HERE");
//        }
        Tuple record = bmPage.getRecord(new RID(pageno, 0));
    }

    @Test
    public void name2() throws Exception {
        new SystemDefs("tempdbname",100000,100,"Clock");
        LinkedList<Boolean> cursorBuffer = new LinkedList<>();
        cursorBuffer.addAll(Arrays.asList(true,false,false,true,false,false,true,true,true,true,false,true));

        Page apage = new Page();
        PageId pageId = new PageId();
        pageId = BitMapFile.newPage(apage, 1);
        HFPage hfpage = new HFPage();
        hfpage.init(pageId, apage);

        hfpage.insertRecord(BitMapFile.toBytes(BitMapFile.toBooleanArray(cursorBuffer)));
        System.out.println("hfpage: "+hfpage.getSlotCnt());

        BitMapFile.unpinPage(hfpage.getCurPage(), true);

        Page page = BitMapFile.pinPage(hfpage.getCurPage());
        HFPage hfPage1 = new HFPage(page);
        System.out.println("hfpage: "+hfPage1.getSlotCnt());
    }

    @Test
    public void hugeBitMap() throws Exception {
        new SystemDefs("tempdbname",100000,100,"Clock");
        BitMapCreator mapCreator = new BitMapCreator("asdfasdf", null, 1, new ValueStrClass(""));
        Random rn = new Random();
        int min = 0;
        int max = 5;

        do{
            for (int i = 0; i < 8000; i++) {
                int num = rn.nextInt(max - min + 1) + min;
                byte[] data = new byte[4];
                Convert.setIntValue(num, 0, data);
                mapCreator.push(AttrType.attrInteger, data);
            }
            mapCreator.checkPoint();
        } while (mapCreator.hasMore());
        mapCreator.close();

        BM bm = new BM();
//        bm.printBitMap(mapCreator.getBitMapFile().getHeaderPage());
    }

    @Test
    public void booleanByteConversion(){
        byte a = (byte) 0;
        byte b = (byte) 1;
        System.out.println("b:"+b);
        System.out.println("a:"+a);

        int a1 = a;
        int b1 = b;

        System.out.println("a1:"+a1);
        System.out.println("b1:"+b1);


    }
}