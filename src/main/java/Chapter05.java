import javafx.util.Pair;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/8/19
 * <p/>
 * package_name:PACKAGE_NAME
 * <p/>
 * project: my-redis-in-action
 */
public class Chapter05 {
    // 日志级别
    private static final String DEBUG = "debug";
    private static final String INFO = "info";
    private static final String WARNING = "warning";
    private static final String ERROR = "error";
    private static final String CRITICAL = "critical";

    private static final Collator COLLATOR = Collator.getInstance();

    private static final SimpleDateFormat TIMESTAMP = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00");

    static {
        // 协调世界时，又称世界统一时间，世界标准时间，国际协调时间，简称UTC
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }


    @Test
    public void testRecentLog() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);
        for (int i = 0; i < 5; i++) {
            logRecent(conn, "test", "this is test message " + i);
        }
        List<String> recentLogs = conn.lrange("recent:test:" + INFO, 0, -1);
        if (recentLogs != null && !recentLogs.isEmpty()) {
            for (String recentLog : recentLogs) {
                System.out.println(recentLog);
            }
        }
    }


    @Test
    public void testCommonLog() {
        Jedis conn = new Jedis("localhost");
        conn.select(0);
        logCommon(conn, "test", "this is test message " + 1, INFO, 100);
        printRedisData(conn);
        List<String> lrange = conn.lrange("recent:test:" + INFO, 0, -1);
        for (String s : lrange) {
            System.out.println(s);
        }
        System.out.println(conn.zscore("common:test:" + INFO, "this is test message " + 1));
    }


    private void printRedisData(Jedis conn) {
        Set<String> keys = conn.keys("*");
        for (String key : keys) {
            System.out.println(key);
        }
    }


    /**
     * 将最新日志以INFO级别记录到redis
     *
     * @param conn    redis连接
     * @param name    消息名
     * @param massage 内容
     */
    private void logRecent(Jedis conn, String name, String massage) {
        logRecent(conn, name, massage, INFO);
    }


    /**
     * 将最新日志记录到redis
     *
     * @param conn     redis连接
     * @param name     消息名
     * @param message  消息内容
     * @param severity 级别
     */
    private void logRecent(Jedis conn, String name, String message, String severity) {
        String key = "recent:" + name + ":" + severity;
        // 使用流水线将通信往返次数降至一次
        /*
        在需要执行大量命令的情况下，
        即使命令实际上并不需要放在事务里面执行，
        但是为了通过一次发送所有命令来减少通信次数并降低延迟值，
        用户也可能会将命令包裹在MULTI和EXEC里面执行。
        将命令包裹在MULTI和EXEC里面执行的缺点在于，
        它们会消耗资源，并且可能会导致其他重要得命令被延迟执行。
        如果我们不需要以事务的方式执行命令，那么可以使用流水线来代替
        “将命令包裹在MULTI和EXEC里面”。
         */
        Pipeline pip = conn.pipelined();
        // 从左边插入一个日志信息
        pip.lpush(key, TIMESTAMP.format(new Date()) + " " + message);
        // 对日志列表进行修剪，只保留最新的100条信息
        /*
        LTRIM key_name start end 对列表进行修剪，
        只保留从start（含）到end（含）偏移量的元素。
         */
        pip.ltrim(key, 0, 99);
        // 批量执行流水，之前的版本使用的是exec()方法，
        // 但是exec()方法执行时如果之前没有开启一个事务，就会报错。
        // 因此修复该bug并使用sync()方法，
        // sync()仅仅是批量执行多个命令不保证事务，
        // 可以降低redis客户端和服务器之间的连接频率，
        // 如果用户需要向redis发送多个命令，并且对于这些命令来说
        // 一个命令的执行结果不会影响另一个命令的输入，
        // 而且这些命令也不需要以事务的方法来执行，那么可以使用该方法。
//        pip.exec();
        pip.sync();
    }

    /**
     * 记录并轮换最常见日志
     * <p/>
     * 将消息作为成员存储到有序集合里，
     * 并将消息出现的频率设置为成员的分值。
     * 为了确保我们看见的常见消息都是最新的，
     * 程序会以每小时一次的频率对消息进行轮换，
     * 并在轮换日志的时候保留上一个小时记录的
     * 常见消息，防止没有任何消息存在的情况出现。
     *
     * @param conn     jedis连接
     * @param name     消息名
     * @param message  消息内容
     * @param severity 消息级别
     * @param timeOut  记录最常见消息方法的执行超时时间
     */
    private void logCommon(Jedis conn, String name, String message, String severity, int timeOut) {
        // 负责存储近期常见日志消息的键
        String commonDesc = "common:" + name + ":" + severity;
        // 程序每小时轮换一次日志，因为使用一个键来记录当前所处的小时数
        String startKey = commonDesc + ":start";
        // 每次记录常用日志的超时时间
        long end = System.currentTimeMillis() + timeOut;
        while (System.currentTimeMillis() < end) {
            // 对记录当前小时数的键进行监视，确保轮换操作可以正确地执行
            conn.watch(startKey);
            String hourStart = ISO_FORMAT.format(new Date());

            String existing = conn.get(startKey);
            System.out.println(hourStart);
            System.out.println(existing);
            Transaction trans = conn.multi();
            // 如果常见日志消息列表记录的是上一个小时的日志
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0) {
                // 把当前的常见日志消息置为上一个小时的常见消息
                trans.rename(commonDesc, commonDesc + ":last");
                // 把记录当前日志小时数的键更名为上一个小时常见日志小时数的键
                trans.rename(startKey, commonDesc + ":pstart");
                // 设置日志所处的小时数
                trans.set(startKey, hourStart);
            } else {
                trans.set(startKey, hourStart);
            }
            // 为有序集commonDesc的message成员的分值加上1
            trans.zincrby(commonDesc, 1, message);

            // 记录最新日志
            String recentDesc = "recent:" + name + ":" + severity;
            trans.lpush(recentDesc, TIMESTAMP.format(new Date()) + " " + message);
            trans.ltrim(recentDesc, 0, 99);
            // 提交从开始事务后执行的一系列命令
            List<Object> result = trans.exec();
            // null 表示因为监视的key在监视后有改变，事务被放弃执行
            if (result == null) {
                continue;
            }
            return;
        }
    }

    private static final int[] PRECISION = new int[]{1, 5, 60, 300, 3600, 18000, 86400};

    private void updateCounter(Jedis conn, String name, int count) {
        updateCounter(conn, name, count, System.currentTimeMillis() / 1000);
    }

    /**
     * 利用计数器记录数据和记录计数器。
     * 针对计数器系统，我们需要计数器记录计数信息，
     * 同时还需要一个计数器集合用以记录正在使用的计数器信息。
     * 1.计数器：以网站点击量计数器为例，计数器以不同的时间精度存储最新的120个数据样本；
     * 2.计数器集合：计数器集合需要元素不能重复并且有序，我们可以使用有序序列作为数据结构，
     * 有序集合用计数器名称及时间精度组成成员，而所有的成员分值都为0。
     * <p/>
     * 注：当有序集合中分值都相等时，Redis将根据成员名称进行排序。
     *
     * @param conn  redis连接
     * @param name  计数器名称
     * @param count 计数次数
     * @param now   计数时的时间
     */
    private void updateCounter(Jedis conn, String name, int count, long now) {
        // multi标记一个事务块的开始
        Transaction trans = conn.multi();
        for (int i : PRECISION) {
            long pnow = (now / i) * i;
            String hash = String.valueOf(pnow) + ":" + name;
            // 把计数器记入“已有计数器”的有序集合，并将其分值设置为0，便于以后执行清理动作
            trans.zadd("known:", 0, hash);
            // 给定的计数器更新计数信息
            trans.hincrBy("count:" + hash, String.valueOf(pnow), count);
        }
        // 执行一个事务块中的一系列命令
        trans.exec();
    }


    /**
     * 获取给定精度的计数器记录数据集
     * @param conn
     * @param name
     * @param precision
     * @return
     */
    private List<Pair<Integer, Integer>> getCounters(Jedis conn, String name, int precision) {

        return null;
    }


}
