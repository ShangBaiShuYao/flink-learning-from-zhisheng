package com.shangbaishuyao.app;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
/**
 * Author: shangbaishuyao
 * Date: 13:39 2021/5/23
 * Desc: Java 实现获取指定文件夹下面的所有文件并对文件重命名
 */
public class TestGetFiles {
    /**
     * 获取一个文件夹下的所有文件全路径
     *  @param path 文件夹路径
     *  @param listFileName 存储文件名
     *
     */
    public static void getAllFileName(String path, ArrayList<String> listFileName) {
        File file = new File(path);
        File[] files = file.listFiles();
        String[] names = file.list();
        if (names != null) {
            String[] completNames = new String[names.length];
            for (int i = 0; i < names.length; i++) {
                completNames[i] = path + names[i];
            }
            listFileName.addAll(Arrays.asList(completNames));
        }
        for (File a : files) {
            //如果文件夹下有子文件夹，获取子文件夹下的所有文件全路径。
            if (a.isDirectory()) {
                getAllFileName(a.getAbsolutePath() + "\\", listFileName);
            }
        }
    }

    /**
     * 重命名文件
     * @param filePath 重命名后文件的目标目录
     * @param fileName 重命名前的完整文件路径
     * @return
     */
    public static void renameFile(String filePath, String fileName) {
        SimpleDateFormat fmdate = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String oldFileName = fileName;
        File oldFile = new File(oldFileName);
        String newFileName = filePath+File.separator+fmdate.format(new Date())+"."+fileName.split("\\.")[1];
        File newFile = new File(newFileName);
        if (oldFile.exists() && oldFile.isFile()) {
            oldFile.renameTo(newFile);
        }
    }


    public static void main(String[] args) {
        ArrayList<String> listFileName = new ArrayList<String>();
        getAllFileName("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\out\\2021-05-22--19\\", listFileName);
        for (String name : listFileName) {
            System.out.println(name);
            renameFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\out\\2021-05-22--19\\", name);
        }
    }
}
