package com.shangbaishuyao.function;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
//import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shangbaishuyao
 * @create 2021-09-15 上午8:55
 * @description: IPTV6 预测方法 <br/>
 */
// TODO define input and output types, e.g. "double,double,double,double,double->double".
//@Slf4j
@Resolve({"double,double,double,double,double->double"})
public class IPTV6UDTF extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {}

    @Override
    public void process(Object[] args) throws UDFException {

        double[] valueX = null;
        double[] valueY = null;
        //获取参数
        Double firstValue = (Double) args[0];
        Double secondValue = (Double) args[1];
        Double thirdValue = (Double) args[2];
        Double fourthValue = (Double) args[3];
        Double fifthValue = (Double) args[4];

        if (args.length < 5 || firstValue == null || secondValue == null || thirdValue == null){
            //返回值
            forward("请输出4或5个参数且索引1-4位不为null的值来预测IPTV6的值!");
//            log.info("请输出4或5个参数且索引1-4位不为null的值来预测IPTV6的值!");
        }

        if (args.length == 5 && fifthValue != null){
            //设置值
            valueX = new double[]{1, 2, 3, 4, 5};
            valueY = new double[]{firstValue, secondValue, thirdValue, fourthValue, fifthValue};
        }else {
            //设置值
            valueX = new double[]{1, 2, 3, 4};
            valueY = new double[]{firstValue, secondValue, thirdValue, fourthValue};
        }

        //获取x的平均值
        double averageX = doubleArrAverage(valueX);
        //获取y的平均值
        double averageY = doubleArrAverage(valueY);

        //计算Sxx =∑(X-averageX)²
        double sxx = SxxOrYY(valueX, averageX);
        //计算Syy =∑(Y-averageY)²
        double syy = SxxOrYY(valueY, averageY);
        //计算Sxy =∑(X-averageX)*∑(Y-averageY)²
        double sxy = Sxy(valueX, valueY, averageX, averageY);

        //推导公式 b1 = Sxy / Sxx  and  b0 = averageY-b1*averageX
        double b1 = sxy / sxx;
        double b0 = averageY - b1 * averageX;

        //获得推动公式
//        log.info("IPTV6 formula is derived :" + "y =" + b0 + "+" + b1 +"* x");

        //推导预测值 IPTV6预测
        double IPTV6Value = 6 * b1 + b0;

        //返回值
        forward(IPTV6Value);
    }

    @Override
    public void close() throws UDFException {}

    /**
     * <p>
     * 计算数组之和的平均值
     * </p>
     * @param arr
     * @return
     */
    public static double doubleArrAverage(double[] arr) {
        double sum = 0;
        for(int i = 0;i < arr.length; i++) {
            sum += arr[i];
        }
        return sum / arr.length;
    }

    /**
     * <p>
     * 计算 Sxx =∑(X-averageX)² or Syy =∑(Y-averageY)²
     * </p>
     * @param valueXorY
     * @param averageXorY
     * @return Sxx
     */
    public static double SxxOrYY(double[] valueXorY,double averageXorY) {
        List XDoubleList = new ArrayList();
        for (int i= 0; i<valueXorY.length ; i++){
            double squareError = (valueXorY[i] - averageXorY) * (valueXorY[i] - averageXorY);
//            log.info("For each x minus the mean square value of x :" + squareError);
            XDoubleList.add(squareError);
        }
        double Sxx = sumList(XDoubleList);
        return Sxx;
    }

    /**
     * <p>
     * 计算 Sxx =∑(X-averageX)² or Syy =∑(Y-averageY)²
     * </p>
     * @param valueX
     * @param valueY
     * @param averageX
     * @param averageY
     * @return Sxx
     */
    public static double Sxy(double[] valueX,double[] valueY,double averageX,double averageY ) {
        List XDoubleList = new ArrayList();
        for (int i= 0; i<valueX.length ; i++){
            double squareError = (valueX[i] - averageX) * (valueY[i] - averageY);
//            log.info("For each x minus the mean square value of x :" + squareError);
            XDoubleList.add(squareError);
        }
        double Sxx = sumList(XDoubleList);
        return Sxx;
    }

    /**
     * <p>
     * List中的数值相加
     * </p>
     * @param numbers
     * @return sum
     */
    public static double sumList(List<Double> numbers) {
        double sum = 0;
        // 遍历
        for (int i = 0; i < numbers.size(); i++) {
            sum = sum + numbers.get(i);
        }
        return sum;
    }
}