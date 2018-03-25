package bitmap;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;
import heap.*;

public class BMPage extends HFPage{

    int keyType;

    public BitMapPage()
    { }

    public BitMapPage(Page page, int keyType)
    {
        try {
            super(page);
            this.keyType = keyType;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte[] getBMpageArray()
    {
        return this.getHFpageArray();
    }

    public void getBMpageArray(byte [] array)
    {
        this.data=array;
    }




}
