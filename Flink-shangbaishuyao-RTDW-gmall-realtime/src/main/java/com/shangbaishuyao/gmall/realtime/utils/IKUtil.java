package com.shangbaishuyao.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: 上白书妖
 * Date: 2021/2/26
 * Desc: IK分词器分词工具类
 */
public class IKUtil {
    //分词    将字符串进行分词，将分词之后的结果放到一个集合中返回
    public static List<String> analyze(String text){
        List<String> wordList = new ArrayList<>();
        //将字符串转换为字符输入流
        StringReader sr = new StringReader(text);
        //创建分词器对象    ik 分词器有两种分词模式：ik_max_word 和 ik_smart 模式。 smart会做一些智能的合并不是吧所以单词都切分出来, 而max_word切分所有单词
        IKSegmenter ikSegmenter = new IKSegmenter(sr, true);//true代表智能
        // Lexeme  是分词后的一个单词对象
        Lexeme lexeme = null;
        //通过循环，获取分词后的数据. 因为你确定不了有多少个数据.
        while(true){
            try {
                //获取一个单词
                if((lexeme = ikSegmenter.next())!=null){
                    String word = lexeme.getLexemeText();
                    wordList.add(word);
                }else{
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return wordList;
    }

    //测试IK分词器能否达到分词的效果
    public static void main(String[] args) {
        System.out.println(IKUtil.analyze("此时正值暮光时分，那缕缕似浮云般冉冉上升的农家房屋顶上的炊烟，那由牧童吹着笛赶着回来的耕牛发出的“哞哞”声，还有那农人扛着锄头回归时叱喝出来的充溢着山野粗犷的没有韵律不成调的乡歌，勾勒出一幅山村平静生活如同“世外桃源”般的暮归图。"));
    }
}
