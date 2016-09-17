package chapter06.queue;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/17
 * <p/>
 * package_name:chapter06.queue
 * <p/>
 * project: my-redis-in-action
 */
public class WorkerWatchQueue {

    @Test
    public void test() {
        Jedis conn = new Jedis("localhost");
        conn.select(0);
        registerTask(conn,"aoo","1aoo");
        registerTask(conn,"boo","2boo");
        List<String> queueKeys = new ArrayList<String>(3);
        queueKeys.add("coo");
        queueKeys.add("hoo");
        queueKeys.add("aoo");
        workerWatchQueue(conn, queueKeys, new CallBackHandle() {
            @Override
            void execute(String jsonStr) {
                System.out.println(jsonStr);
            }
        });
    }

    /**
     * 获取待执行任务
     *
     * @param conn      redis连接
     * @param queuesKey 队列集合
     * @param handle    执行任务的回调函数
     */
    private void workerWatchQueue(Jedis conn, List<String> queuesKey, CallBackHandle handle) {
        List<String> taskList = conn.blpop(10, queuesKey.toArray(new String[queuesKey.size()]));
        handle.execute(taskList.get(1));
    }

    /**
     * 注册任务
     *
     * @param conn    jedis连接
     * @param queue   任务队列
     * @param jsonStr 任务执行所需参数的json字符串
     */
    private void registerTask(Jedis conn, String queue, String jsonStr) {
        conn.rpush(queue, jsonStr);
    }


    private abstract class CallBackHandle {
        abstract void execute(String jsonStr);
    }
}
