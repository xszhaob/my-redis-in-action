package chapter08;

import chapter06.lock.BetterLock;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/10/23
 * <p/>
 * package_name:chapter08
 * <p/>
 * project: my-redis-in-action
 */
public class MyTwitter {
    private static final String USER_KEY = "user:";
    private static final String USERS_KEY = "users:";
    private static final String STATUS_KEY = "status:";
    private static final String HOME_KEY = "home:";
    private static final String PROFILE_KEY = "profile:";
    private static final String FOLLOWERS_KEY = "followers:";
    private static final String FOLLOWING_KEY = "following:";
    private static final String USER_ID = "user:id:";
    private static final String STATUS_ID = "status:id:";
    private static final BetterLock LOCK = new BetterLock();

    public String createUser(Jedis conn, String login, String name) {
        String lowerLogin = login.toLowerCase();
        String lockId = LOCK.acquireLockWithTimeOut(conn, "user:" + lowerLogin, 10, 5);
        try {
            // 锁失败说明名字被注册，且根据用户名库校验登录名是否被其他人注册
            if (lockId == null || conn.hget("users:", lowerLogin) != null) {
                return null;
            }
            // 获取用户id
            String userId = conn.incr(USER_ID).toString();

            Transaction trans = conn.multi();
            // 插入用户名库
            trans.hset(USERS_KEY, lowerLogin, userId);
            Map<String, String> values = new HashMap<String, String>();
            values.put("login", login);
            values.put("id", userId);
            values.put("name", name);
            values.put("followers", "0");
            values.put("following", "0");
            values.put("posts", "0");
            values.put("signUp", System.currentTimeMillis() + "");
            trans.hmset(USER_KEY + userId, values);
            trans.exec();
            return userId;
        } finally {
            if (lockId != null) {
                LOCK.releaseLock(conn, "user:" + lowerLogin, lockId);
            }
        }
    }


    public String createStatus(Jedis conn, String uid, String message, Map<String, String> data) {
        Transaction trans = conn.multi();
        trans.hget(USER_KEY + uid, "login");
        trans.incr(STATUS_ID);
        List<Object> execRet = trans.exec();
        if (execRet.get(0) == null) {
            return null;
        }
        if (data == null) {
            data = new HashMap<String, String>();
        }
        data.put("message", message);
        data.put("posted", System.currentTimeMillis() + "");
        data.put("id", execRet.get(1).toString());
        data.put("uid", uid);
        data.put("login", execRet.get(0).toString());
        trans = conn.multi();
        trans.hmset(STATUS_KEY + execRet.get(1), data);
        trans.hincrBy(USER_KEY + uid, "posts", 1);
        trans.exec();
        return execRet.get(1).toString();
    }
}
