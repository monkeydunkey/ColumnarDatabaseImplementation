package tests;

import bitmap.ColumnarBitmapEquiJoins;
import columnar.Columnarfile;
import diskmgr.pcounter;
import global.AttrType;
import global.TID;
import heap.*;
import iterator.ColumnarIndexScan;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BitMapJoinCommand {
    public static void run(String[] split) {
        try {
            pcounter.initialize();
            //bmj COLUMNDB          OUTERFILE                   INNERFILE                   OUTERCONST       INNERCONST      EQUICONST       [TARGETCOLUMNS]     NUMBUF
            //bmj column_db_name    columnar_file_name_outter    columnar_file_name_inner   {A = 1,A = 2}   {B = 1,B = 2}   {T1.A = T2.B}   [T1.A T1.B T2.B]    50
            // T1 = outer
            // T2 = inner

            BitMapJoinInput bitMapJoinInput = BitMapJoinInput.parse(split);


            Columnarfile outterColumnarfile = new Columnarfile(bitMapJoinInput.getOuterFile());
            Columnarfile innerColumnarfile = new Columnarfile(bitMapJoinInput.getInnerFile());

            bitMapJoinInput.finishParsing(outterColumnarfile,innerColumnarfile);

            ColumnarBitmapEquiJoins columnarBitmapEquiJoins = new ColumnarBitmapEquiJoins(
                    outterColumnarfile.type,
                    outterColumnarfile.type.length,
                    new short[]{(short) outterColumnarfile.STRINGSIZE},
                    innerColumnarfile.type,
                    innerColumnarfile.type.length,
                    new short[]{(short) innerColumnarfile.STRINGSIZE},
                    bitMapJoinInput.getNumbufInt(),
                    bitMapJoinInput.getInnerFile(),
                    bitMapJoinInput.getLeftJoinFieldIndex(),
                    bitMapJoinInput.getInnerFile(),
                    bitMapJoinInput.getRightJoinFieldIndex(),
                    bitMapJoinInput.getProjectionList(),
                    bitMapJoinInput.getProjectionList().length
            );

            Tuple newtuple = columnarBitmapEquiJoins.get_next();

            while(newtuple != null) {
                newtuple.print(new AttrType[0]);// todo pass the actual Attr types from projection
                newtuple = columnarBitmapEquiJoins.get_next();
            }
            columnarBitmapEquiJoins.close();


            //todo parse input params

            //todo execute

            System.out.println("BitMapJoin done!");
            System.out.println("Disk read count: " + pcounter.rcounter);
            System.out.println("Disk write count: " + pcounter.wcounter);
        }catch (Exception e){
            System.out.println("Error executing bitMap Join");
            e.printStackTrace();
        }
    }


}
