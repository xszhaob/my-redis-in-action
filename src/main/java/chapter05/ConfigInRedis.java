package chapter05;

import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/11
 * <p/>
 * package_name:chapter05
 * <p/>
 * project: my-redis-in-action
 * <p/>
 * 使用redis存储服务器的配置信息
 */
public class ConfigInRedis {
    private Date lastCheck;
    private boolean isUnderMain = false;
    private static final String IS_UNDER_MAINTENANCE_KEY = "isUnderMaintenance";

    private Map<String, String> configCache = new HashMap<String, String>();
    private Date lastConfig;


    /**
     * 返回服务器是否正在维护。
     * 该方法通过检查<code>IS_UNDER_MAINTENANCE_KEY</code>
     * 键是否为空来判断服务器是否正在维护中。
     * 如果<code>IS_UNDER_MAINTENANCE_KEY</code>键非空，
     * 那么返回true并且认为服务器正在维护中，否则返回false。
     * 为了防止客户不断请求该方法给redis服务器造成较高的负载，
     * 该方法最多会间隔一秒钟从服务其中获取一次
     * <code>IS_UNDER_MAINTENANCE_KEY</code>键的值。
     *
     * @param conn redis连接
     * @return true如果服务器正在维护中，否则返回false
     */
    private boolean isUnderMaintenance(Jedis conn) {
        Date now = new Date();
        if (lastCheck == null || (now.getTime() - lastCheck.getTime()) > 1000) {
            isUnderMain = conn.get(IS_UNDER_MAINTENANCE_KEY) != null;
            lastCheck = now;
        }
        return isUnderMain;
    }

    /**
     * 设置配置信息
     *
     * @param conn       redis连接
     * @param type       服务器类型
     * @param serverName 服务器名称
     * @param config     配置信息
     */
    private void setConfig(Jedis conn, String type, String serverName, String config) {
        String key = "config:" + type + ":" + serverName;
        conn.set(key, config);
    }

    /**
     * 获取配置参数。
     * 和{@link ConfigInRedis#isUnderMaintenance}类似，
     * 为了降低服务的负载，该方法将会把获取结果最多缓存1秒钟。
     *
     * @param conn       redis连接
     * @param type       服务器类型
     * @param serverName 服务器名称
     * @return 配置参数
     */
    private String getConfig(Jedis conn, String type, String serverName) {
        String key = "config:" + type + ":" + serverName;
        String result = configCache.get(key);
        Date now = new Date();
        if (result == null || lastConfig == null || (now.getTime() - lastConfig.getTime()) > 1000) {
            result = conn.get(key);
            configCache.put(key, result);
            lastConfig = new Date();
        }
        return result;
    }
}
