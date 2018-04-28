package tests;

import iterator.FldSpec;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BitMapJoinInput {
    //bmj COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST EQUICONST [TARGETCOLUMNS] NUMBUF

    private String columnDB;
    private String outerFile;
    private String innerFile;
    private String outerConst;
    private String innerConst;
    private String equiConst;
    private String targetColumns;
    private String numbuf;

    public String getColumnDB() {
        return columnDB;
    }

    public void setColumnDB(String columnDB) {
        this.columnDB = columnDB;
    }

    public String getOuterFile() {
        return outerFile;
    }

    public void setOuterFile(String outerFile) {
        this.outerFile = outerFile;
    }

    public String getInnerFile() {
        return innerFile;
    }

    public void setInnerFile(String innerFile) {
        this.innerFile = innerFile;
    }

    public String getOuterConst() {
        return outerConst;
    }

    public void setOuterConst(String outerConst) {
        this.outerConst = outerConst;
    }

    public String getInnerConst() {
        return innerConst;
    }

    public void setInnerConst(String innerConst) {
        this.innerConst = innerConst;
    }

    public String getEquiConst() {
        return equiConst;
    }

    public void setEquiConst(String equiConst) {
        this.equiConst = equiConst;
    }

    public String getTargetColumns() {
        return targetColumns;
    }

    public void setTargetColumns(String targetColumns) {
        this.targetColumns = targetColumns;
    }

    public String getNumbuf() {
        return numbuf;
    }

    public void setNumbuf(String numbuf) {
        this.numbuf = numbuf;
    }

    public static BitMapJoinInput parse(String[] split) {
        final String regex = "(\\w+) (\\w+) (\\w+) (\\{.+?\\}) (\\{.+?\\}) (\\{.+?\\}) (\\[.+?\\])\\s(.+$)";
        final String inputString = String.join(" ", split);

        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(inputString);

        BitMapJoinInput bitMapJoinInput = new BitMapJoinInput();
        while (matcher.find()) {
            bitMapJoinInput.setColumnDB(matcher.group(1));
            bitMapJoinInput.setOuterFile(matcher.group(2));
            bitMapJoinInput.setInnerFile(matcher.group(3));
            bitMapJoinInput.setOuterConst(matcher.group(4));
            bitMapJoinInput.setInnerConst(matcher.group(5));
            bitMapJoinInput.setEquiConst(matcher.group(6));
            bitMapJoinInput.setTargetColumns(matcher.group(7));
            bitMapJoinInput.setNumbuf(matcher.group(8));
        }

        return bitMapJoinInput;
    }

    public FldSpec[] getProjectionList() {
        return new FldSpec[0];//todo
    }

    public int getNumbufInt() {
        return Integer.parseInt(getNumbuf());
    }

    public int getLeftJoinFieldIndex() {
        return 0;//todo
    }

    public int getRightJoinFieldIndex() {
        return 0;//todo
    }
}
