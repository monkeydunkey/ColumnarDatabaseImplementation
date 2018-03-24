package bitmap;

import btree.BTreeHeaderPage;
import btree.ConstructPageException;
import diskmgr.Page;
import global.PageId;
import global.SystemDefs;

import java.io.IOException;

public class BitMapHeaderPage extends BMPage{

    private PageId _rootId;

    /**
     * @see btree.BTreeHeaderPage#BTreeHeaderPage()
     * @throws ConstructPageException
     */
    public BitMapHeaderPage()
            throws ConstructPageException {
        super();
        try {
            Page apage = new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
            if (pageId == null)
                throw new ConstructPageException(null, "new page failed");
            this.init(pageId, apage);

        } catch (Exception e) {
            throw new ConstructPageException(e, "construct header page failed");
        }
    }

    /**
     * @see btree.BTreeHeaderPage#BTreeHeaderPage(PageId pageno)
     * @param pageno
     * @throws ConstructPageException
     */
    public BitMapHeaderPage(PageId pageno) throws ConstructPageException {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }

    /**
     * @see btree.BTreeHeaderPage#set_rootId(PageId rootId)
     * @param rootId
     * @throws IOException
     */
    public void set_rootId(PageId rootId) throws IOException {
        setNextPage(rootId);
    }

    /**
     * @see btree.BTreeHeaderPage#getPageId()
     * @return
     * @throws IOException
     */
    PageId getPageId()
            throws IOException {
        return getCurPage();
    }
}
