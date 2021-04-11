package com.shangbaishuyao.gmall.realtime.app.Function;

import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.bean.TableProcess;
import com.shangbaishuyao.gmall.realtime.common.GmallConfig;
import com.shangbaishuyao.gmall.realtime.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Desc: 动态分流,配置表处理函数<br/>
 * ProcessFunction是一个抽象类, 我得去继承一下.
 *
 * 其实map集合里面放的是一个一个的k-v键值对,但是每一个k-v键值对他又封装了一个entry对象. 多个entry组成了一个Set集合,即entrySet集合.
 * @Author: 上白书妖
 * @Date: 0:05 2021/4/11
 */
public class TableProcessFunction extends ProcessFunction<JSONObject,JSONObject> {

    //侧输出流标记
    private final OutputTag<JSONObject> hbaseOutputTag;
    //声明Phoenix的连接对象
    Connection conn = null;
    //用于在内存中存放配置表信息的Map <表名：操作,tableProcess>
    private Map<String, TableProcess> tableProcessMap = new HashMap<>();
    //用于在内存中存放已经处理过的表（在phoenix中已经建过的表）
    private Set<String> existsTables = new HashSet<>();

    //实例化函数对象时,也能将侧输出流标签进行赋值
    public TableProcessFunction(OutputTag<JSONObject> hbaseOutputTag) {
        this.hbaseOutputTag=hbaseOutputTag;
    }
    //周期性要做什么事情呢?
    //①查询mysql中的配置信息 ②使用JDBC获取Phoenix连接,检查Hbase是否存在该表,不存在创建 ③将事实表存放在map集合中,供processElement调用分析.
    //首次执行open方法,open方法只执行一次.
    //TODO open
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //初始化配置表信息,查询mysql表中的配置
        refreshMeta();

        //开启一个定时任务
        // 因为配置表的数据可能会发生变化，每隔一段时间就从配置表中查询一次数据，更新到map，并检查建表
        //从现在起过delay毫秒后，每隔period执行一次
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 5000, 5000);
    }

    //TODO processElement 大量数据过来分析
    @Override
    public void processElement(JSONObject value, Context context, Collector<JSONObject> out) throws Exception {
        //获取表名
        String table = value.getString("table");
        //获取操作类型
        String type = value.getString("type");
        //TODO 注意：问题修复  如果使用Maxwell的Bootstrap同步历史数据,这个时候它的操作类型叫bootstrap-insert
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            value.put("type", type);
        }
        //如果在我们的内存中(tableProcessMap)有配置信息了.我们才做业务数据的维度表和事实表的判断.
        if (tableProcessMap != null && tableProcessMap.size() > 0) {
            //根据表名和操作类型拼接key
            String key = table + ":" + type;
            //从内存的配置Map中获取当前key对象的配置信息
            TableProcess tableProcess = tableProcessMap.get(key);
            //如果获取到了该元素对应的配置信息
            if (tableProcess != null) {
                //TODO 表示我这条记录要发往何处
                //获取sinkTable，指明当前这条数据应该发往何处  如果是维度数据，那么对应的是phoenix中的表名；如果是事实数据，对应的是kafka的主题名
                value.put("sink_table", tableProcess.getSinkTable());
                String sinkColumns = tableProcess.getSinkColumns();
                //如果指定了sinkColumn，需要对保留的字段进行过滤处理
                //过滤不关心的字段
                if (sinkColumns != null && sinkColumns.length() > 0) {
                    filterColumn(value.getJSONObject("data"), sinkColumns);
                }
            } else {
                System.out.println("NO this Key:" + key + "in MySQL");
            }

            //TODO 根据sinkType，将数据输出到不同的流
            if(tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                //如果sinkType = hbase ，说明是维度数据，通过侧输出流输出
                context.output(hbaseOutputTag,value);
            }else if(tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                //如果sinkType = kafka ，说明是事实数据，通过主流输出
                out.collect(value);
            }
        }
    }
    //其实map集合里面放的是一个一个的k-v键值对,但是每一个k-v键值对他又封装了一个entry对象. 多个entry组成了一个Set集合,即entrySet.
    //TODO 过滤不关心的字段,只保留配置表里面指定的字段
    //对Data中数据进行进行过滤
    private void filterColumn(JSONObject data, String sinkColumns) {
        //sinkColumns 表示要保留那些列(属性)     id,out_trade_no,order_id
        String[] cols = sinkColumns.split(",");
        //数组里面没有包含的方法Contains,但是集合里面有包含的方法叫Contains.
        //为了判断是否包含某个元素,将数组转换为集合，为了判断集合中是否包含某个元素
        List<String> columnList = Arrays.asList(cols);

        //json将也是把他里面的一个个个键值对属性封装成entry对象.多个entry对象就是entrySet集合. 可以通过entry对象拿到entrySet集合
        //获取json对象中封装的一个个键值对   每个键值对封装为Entry类型
        Set<Map.Entry<String, Object>> entrySet = data.entrySet(); //这个entrySet和map集合的entrySet是一样的.

        //TODO 因为你想将不包含的删掉,即在遍历的过程中删掉,但是集合是不给删除的.所以你必须拿到一个迭代器.通过迭代器删除.
        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();

        //数组里面没有包含的方法Contains,但是集合里面有包含的方法叫Contains.
        //TODO 这个条件是,当迭代器有下一个元素的时候. 这是我来做一个遍历.
        for (;it.hasNext();) {//这就是普通的for循环
            //TODO 取出下一个元素.
            Map.Entry<String, Object> entry = it.next();
            //判断我当前集合中是否包含我这个键值对的key
            if(!columnList.contains(entry.getKey())){
                //如果不包含,我不想保留,即删掉
                //TODO 如果你想要在遍历的过程中做删除,你应该使用迭代器来做删除操作.
                it.remove();
            }
        }
    }

    /**
     * 查询配置表
     * 从MySQL数据库配置表中查询配置信息 <br/>
     */
    private void refreshMeta() {
        //========1.从MySQL数据库配置表中查询配置信息============
        System.out.println("查询配置表信息");
        List<TableProcess> tableProcessList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
        //对查询出来的结果集进行遍历
        for (TableProcess tableProcess : tableProcessList) {
            //获取源表表名
            String sourceTable = tableProcess.getSourceTable();
            //获取操作类型
            String operateType = tableProcess.getOperateType();
            //输出类型      hbase|kafka
            String sinkType = tableProcess.getSinkType();
            //输出目的地表名或者主题名
            String sinkTable = tableProcess.getSinkTable();
            //输出字段
            String sinkColumns = tableProcess.getSinkColumns();
            //表的主键
            String sinkPk = tableProcess.getSinkPk();
            //建表扩展语句
            String sinkExtend = tableProcess.getSinkExtend();
            //拼接保存配置的key (表名+类型)
            String key = sourceTable + ":" + operateType;

            //========2.将从配置表中查询到配置信息，保存到内存的map集合中=============
            tableProcessMap.put(key, tableProcess);

            //========3.如果当前配置项是维度配置，需要向Hbase表中保存数据，那么我们需要判断phoenix中是否存在这张表=====================
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
                boolean notExist = existsTables.add(sourceTable);
                //如果在内存Set集合中不存在这个表，那么在Phoenix中创建这种表
                if (notExist) {
                    //检查在Phonix中是否存在这种表
                    //有可能已经存在，只不过是应用缓存被清空，导致当前表没有缓存，这种情况是不需要创建表的
                    //在Phoenix中，表的确不存在，那么需要将表创建出来
                    checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
                }
            }
        }

        //TODO 这其实是一个异常的情况，一定要进行判断
        //如果没有从数据库的配置表中读取到数据
        if (tableProcessMap == null || tableProcessMap.size() == 0) {
            throw new RuntimeException("没有从数据库的配置表中读取到数据");
        }
    }

    /**
     * 在Phoenix中，表的确不存在，那么需要将表创建出来 <br/>
     * @param tableName
     * @param fields
     * @param pk
     * @param ext
     */
    private void checkTable(String tableName, String fields, String pk, String ext) {
        //如果在配置表中，没有配置主键 需要给一个默认主键的值
        if (pk == null) {
            pk = "id";
        }
        //如果在配置表中，没有配置建表扩展 需要给一个默认建表扩展的值
        if (ext == null) {
            ext = "";
        }
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " +
                GmallConfig.HABSE_SCHEMA + "." + tableName + "(");

        //对建表字段进行切分
        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            //判断当前字段是否为主键字段
            if (pk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append("info.").append(field).append(" varchar ");
            }
            if (i < fieldsArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(ext);

        System.out.println("创建Phoenix表的语句:" + createSql);

        //获取Phoenix连接
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(createSql.toString());
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Phoenix建表失败");
                }
            }
        }
    }
}
