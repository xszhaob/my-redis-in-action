import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.text.SimpleDateFormat;
import java.util.Date;

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
    private static final String DEBUG = "debug";
    private static final String INFO = "info";
    private static final String WARNING = "warning";
    private static final String ERROR = "error";
    private static final String CRITICAL = "critical";

    private static final SimpleDateFormat TIMESTAMP = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
    }




    private void logRecent(Jedis conn,String name,String message,String severity) {
        String key = "recent:" + name + ":" + severity;
        Pipeline pip = conn.pipelined();
        pip.lpush(key,TIMESTAMP.format(new Date()) + " " + message);
        pip.ltrim(key,0,99);
        pip.sync();
    }


}
