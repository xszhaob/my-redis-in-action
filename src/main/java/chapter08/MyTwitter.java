package chapter08;

import chapter06.lock.BetterLock;
import redis.clients.jedis.*;

import java.util.*;

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
    private static final long HOME_TIMELINE_SIZE = 1000;
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
        data.put("delete", "false");
        data.put("login", execRet.get(0).toString());
        trans = conn.multi();
        trans.hmset(STATUS_KEY + execRet.get(1), data);
        trans.hincrBy(USER_KEY + uid, "posts", 1);
        trans.exec();
        return execRet.get(1).toString();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String,String>> getStatusMessage(Jedis conn,String uid,String timeLine,int page,int count) {
        if (page == 0) {
            page = 1;
        }
        if (count == 0) {
            count = 30;
        }
        if (timeLine == null || timeLine.isEmpty()) {
            timeLine = HOME_KEY;
        }

        Set<String> statusSet = conn.zrevrange(timeLine + uid, ((page - 1) * count), page * count - 1);
        if (statusSet == null || statusSet.isEmpty()) {
            return new ArrayList<Map<String, String>>();
        }
        List<Map<String,String>> resultList = new ArrayList<Map<String, String>>(statusSet.size());
        Transaction multi = conn.multi();
        for (String s : statusSet) {
            multi.hgetAll(STATUS_KEY + s);
        }
        List<Object> exec = multi.exec();
        for (Object o : exec) {
            Map<String,String> status = (Map<String,String>) o;
            if (!isDelete(status)) {
                resultList.add(status);
            }
        }
        return resultList;
    }


    private boolean isDelete(Map<String,String> status) {
        try {
            return Boolean.parseBoolean(status.get("delete"));
        } catch (Exception ignore) {
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    public boolean followUser(Jedis conn,String uid,String followUserId) {
        String followingKey = FOLLOWING_KEY + uid;
        String followerKey = FOLLOWERS_KEY + followUserId;
        if (conn.zscore(followingKey,followUserId) != null) {
            return false;
        }
        long nowTimeLong = System.currentTimeMillis();
        Transaction trans = conn.multi();
        trans.zadd(followingKey,nowTimeLong,followUserId);
        trans.zadd(followerKey,nowTimeLong,uid);

        trans.zrevrangeWithScores(STATUS_KEY + followUserId, 0, HOME_TIMELINE_SIZE - 1);
        List<Object> exec = trans.exec();
        if (exec == null) {
            return false;
        }
        trans = conn.multi();
        trans.hincrBy(USER_KEY + uid,"following",Long.parseLong(exec.get(0).toString()));
        trans.hincrBy(USER_KEY + uid,"followers",Long.parseLong(exec.get(1).toString()));
        Set<Tuple> followingStatus = (Set<Tuple>) exec.get(2);
        if (followingStatus != null) {
            trans.zadd(HOME_KEY + uid,getScoreMembers(followingStatus));
        }
        trans.zremrangeByRank(HOME_KEY + uid,0,-HOME_TIMELINE_SIZE -1);
        trans.exec();
        return true;
    }

    private Map<String,Double> getScoreMembers(Set<Tuple> tuples) {
        Map<String,Double> result = new HashMap<String, Double>();
        for (Tuple tuple : tuples) {
            result.put(tuple.getElement(),tuple.getScore());
        }
        return result;
    }
}
