package com.excel.util;

import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.*;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @description: 多个Excel合并Sheet
 * @author: wyj
 * @time: 2020/9/18 15:28
 */
public class ExcelUtil {


    public static void main(String[] args) {
        List<String> list = Arrays.asList(
                new File("D:\\test\\.archivetemp王涛（20210303-0331）报销明细(1).xlsx").toString(),
                new File("D:\\test\\.archivetemp王涛（20210401-0430）报销明细(1).xlsx").toString(),
                new File("D:\\test\\.archivetemp王涛（20210501-0530）报销明细(1).xlsx").toString()
        );

        mergexcel(list, ".archivetemp王涛.xlsx", "D:\\test");
        System.out.println("OJBK");
    }

    /**
     * * 合并多个ExcelSheet
     *
     * @param files     文件字符串(file.toString)集合,按顺序进行合并，合并的Excel中Sheet名称不可重复
     * @param excelName 合并后Excel名称(包含后缀.xslx)
     * @param dirPath   存储目录
     * @return
     * @Date: 2020/9/18 15:31
     */
    public static void mergexcel(List<String> files, String excelName, String dirPath) {
        HSSFWorkbook newExcelCreat = new HSSFWorkbook();
        int num = 1;
        // 遍历每个源excel文件，TmpList为源文件的名称集合
        for (String fromExcelName : files) {
            try (InputStream in = new FileInputStream(fromExcelName)) {
                XSSFWorkbook fromExcel = new XSSFWorkbook(in);
                int length = fromExcel.getNumberOfSheets();
                if (length <= 1) {       //长度为1时
                    XSSFSheet oldSheet = fromExcel.getSheetAt(0);
                    HSSFSheet newSheet = newExcelCreat.createSheet(oldSheet.getSheetName() + num);
                    copySheet(newExcelCreat, oldSheet, newSheet);
                } else {
                    for (int i = 0; i < length; i++) {// 遍历每个sheet
                        XSSFSheet oldSheet = fromExcel.getSheetAt(i);
                        HSSFSheet newSheet = newExcelCreat.createSheet(oldSheet.getSheetName());
                        copySheet(newExcelCreat, oldSheet, newSheet);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // 定义新生成的xlxs表格文件
        String allFileName = dirPath + File.separator + excelName;
        try (FileOutputStream fileOut = new FileOutputStream(allFileName)) {
            newExcelCreat.write(fileOut);
            fileOut.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                newExcelCreat.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 合并单元格
     *
     * @param fromSheet
     * @param toSheet
     */
    private static void mergeSheetAllRegion(XSSFSheet fromSheet, HSSFSheet toSheet) {
        int num = fromSheet.getNumMergedRegions();
        CellRangeAddress cellR = null;
        for (int i = 0; i < num; i++) {
            cellR = fromSheet.getMergedRegion(i);
            toSheet.addMergedRegion(cellR);
        }
    }

    /**
     * 复制单元格
     *
     * @param wb
     * @param fromCell
     * @param toCell
     */
    private static void copyCell(HSSFWorkbook wb, HSSFCell fromCell, HSSFCell toCell) {
        HSSFCellStyle newstyle = wb.createCellStyle();
        // 复制单元格样式
        newstyle.cloneStyleFrom(fromCell.getCellStyle());
        // 样式
        toCell.setCellStyle(newstyle);
        if (fromCell.getCellComment() != null) {
            toCell.setCellComment(fromCell.getCellComment());
        }
        // 不同数据类型处理
        CellType fromCellType = fromCell.getCellType();
        toCell.setCellType(CellType.STRING);
        //toCell.setCellFormula(fromCellType);
        if (fromCellType == CellType.NUMERIC) {
            if (DateUtil.isCellDateFormatted(fromCell)) {
                toCell.setCellValue(fromCell.getDateCellValue());
            } else {
                toCell.setCellValue(fromCell.getNumericCellValue());
            }
        } else if (fromCellType == CellType.STRING) {
            toCell.setCellValue(fromCell.getRichStringCellValue());
        } else if (fromCellType == CellType.BLANK) {
            // nothing21
        } else if (fromCellType == CellType.BOOLEAN) {
            toCell.setCellValue(fromCell.getBooleanCellValue());
        } else if (fromCellType == CellType.ERROR) {
            toCell.setCellErrorValue(fromCell.getErrorCellValue());
        } else if (fromCellType == CellType.FORMULA) {
            toCell.setCellFormula(fromCell.getCellFormula());
        } else {
            // nothing29
        }
    }

    /**
     * 行复制功能
     *
     * @param wb
     * @param oldRow
     * @param toRow
     */
    private static void copyRow(HSSFWorkbook wb, HSSFRow oldRow, HSSFRow toRow) {
        toRow.setHeight(oldRow.getHeight());
        for (Iterator cellIt = oldRow.cellIterator(); cellIt.hasNext(); ) {
            HSSFCell tmpCell = (HSSFCell) cellIt.next();
            HSSFCell newCell = toRow.createCell(tmpCell.getColumnIndex());
            copyCell(wb, tmpCell, newCell);
        }
    }

    /**
     * Sheet复制
     *
     * @param wb
     * @param fromSheet
     * @param toSheet
     */
    private static void copySheet(HSSFWorkbook wb, XSSFSheet fromSheet, HSSFSheet toSheet) {
        mergeSheetAllRegion(fromSheet, toSheet);
        // 设置列宽
        int length = fromSheet.getRow(fromSheet.getFirstRowNum()).getLastCellNum();
        for (int i = 0; i <= length; i++) {
            toSheet.setColumnWidth(i, fromSheet.getColumnWidth(i));
        }
        for (Iterator rowIt = fromSheet.rowIterator(); rowIt.hasNext(); ) {
            HSSFRow oldRow = (HSSFRow) rowIt.next();
            HSSFRow newRow = toSheet.createRow(oldRow.getRowNum());
            copyRow(wb, oldRow, newRow);
        }
    }
}
