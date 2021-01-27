package com.shangbaishuyao.sources.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc: 规则 <br/>
 * create by shangbaishuyao on 2020-12-11
 *@Author: 上白书妖
 *@Date: 2020/12/11 13:32
 *
 *
 * Desc: 注解补充: <br/>
 * @Data ： 注在类上，提供类的get、set、equals、hashCode、canEqual、toString方法
 * @AllArgsConstructor ： 注在类上，提供类的全参构造
 * @NoArgsConstructor ： 注在类上，提供类的无参构造
 * @Setter ： 注在属性上，提供 set 方法
 * @Getter ： 注在属性上，提供 get 方法
 * @EqualsAndHashCode ： 注在类上，提供对应的 equals 和 hashCode 方法
 * @Log4j/@Slf4j ： 注在类上，提供对应的 Logger 对象，变量名为 log
 *
 * lombok注解在java进行编译时进行代码的构建，对于java对象的创建工作它可以更优雅，不需要写多余的重复的代码，这对于JAVA开发人员是很重要的，
 * 在出现lombok之后，对象的创建工作更提供Builder方法，它提供在设计数据实体时，对外保持private setter，而对属性的赋值采用Builder的方式，
 * 这种方式最优雅，也更符合封装的原则，不对外公开属性的写操作！
 * @Builder声明实体，表示可以进行Builder方式初始化，@Value注解，表示只公开getter，对所有属性的setter都封闭，即private修饰，所以它不能和@Builder现起用
 * 一般地，我们可以这样设计实体！
 *
 * @Builder(toBuilder = true) //@Builder注解修改原对象的属 性值修改实体，要求实体上添加@Builder(toBuilder=true)
 * @Getter
 * public class UserInfo {
 *   private String name;
 *   private String email;
 *   @MinMoney(message = "金额不能小于0.")
 *   @MaxMoney(value = 10, message = "金额不能大于10.")
 *   private Money price;
 *
 * }
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Rule {
    /**
     * rule id
     */
    private String id;

    /**
     * rule name
     */
    private String name;

    /**
     * rule type
     */
    private String type;

    /**
     * monitor measurement 监测度量
     */
    private String measurement;

    /**
     * rule expression  规则表达
     */
    private String expression;

    /**
     * measurement threshold  测量阈值
     */
    private String threshold;

    /**
     * alert level  警戒级别
     */
    private String level;

    /**
     * rule targetType 规则目标类型
     */
    private String targetType;

    /**
     * rule targetId  跪着目标id
     */
    private String targetId;

    /**
     * notice webhook, only DingDing group rebot here
     * TODO: more notice ways  更多的通知方式
     */
    private String webhook;
}
