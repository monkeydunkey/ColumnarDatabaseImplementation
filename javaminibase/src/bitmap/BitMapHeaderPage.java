package bitmap;

import diskmgr.*;
import global.*;
import heap.*;

import java.io.IOException;

public class BMHeaderPage extends HFPage{

    ArrayList<BMPage> bmPageList;

    public BitMapHeaderPage(PageId pageno)
    {
        super();
        try {

            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**associate the SortedPage instance with the Page instance */
    public BitMapHeaderPage(Page page) {

        super(page);
    }


    /**new a page, and associate the SortedPage instance with the Page instance
     */
    public BitMapHeaderPage( )
            throws ConstructPageException
    {
        super();
        try{
            Page apage=new Page();
            PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
            this.init(pageId, apage);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
