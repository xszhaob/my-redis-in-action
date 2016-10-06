package chapter06.chat;

import bo.zhao.json.MyJsonUtil;
import chapter06.lock.BetterLock;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.io.IOException;
import java.util.*;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/19
 * <p/>
 * package_name:chapter06.chat
 * <p/>
 * project: my-redis-in-action
 */
public class Chat {

    /**
     * 创建聊天组
     *
     * @param conn       redis连接
     * @param sender     发送者
     * @param recipients 聊天组成员
     * @param message    消息
     * @param chatId     聊天组id
     */
    public String createChat(Jedis conn, String sender, Set<String> recipients, String message, String chatId) {
        chatId = chatId == null ? conn.incr("ids:chat:").toString() : chatId;
        recipients.add(sender);

        Transaction trans = conn.multi();
        for (String recipient : recipients) {
            // 聊天组，member是群聊用户
            trans.zadd("chat:" + chatId, 0, recipient);
            // 每个用户在每个聊天中已看信息数量
            trans.zadd("seen:" + recipient, 0, chatId);
        }
        trans.exec();

        return sendMessage(conn,chatId,sender,message);
    }

    /**
     * 发送消息
     *
     * @param conn    redis连接
     * @param chatId  聊天组id
     * @param sender  发送者
     * @param message 消息
     */
    public String sendMessage(Jedis conn, String chatId, String sender, String message) {
        BetterLock lock = new BetterLock();
        String lockId = lock.acquireLockWithTimeOut(conn, "chat:lock:" + chatId, 100, 3);
        if (lockId == null) {
            throw new RuntimeException("Couldn't get the lock");
        }
        try {
            Long messageId = conn.incr("ids:" + chatId);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("messageId", messageId);
            map.put("ts", System.currentTimeMillis());
            map.put("sender", sender);
            map.put("message", message);
            String msg = new ObjectMapper().writeValueAsString(map);
            conn.zadd("msgs:" + chatId, messageId, msg);
            return chatId;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } finally {
            lock.releaseLock(conn, "chat:lock:" + chatId, lockId, 100);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public List<ChatMessages> fetchPendingMessage(Jedis conn, String recipient) throws IOException {
        // 用户在每个组中的已读消息 tuple<chatId,msgId> msgId：已读消息中id最大的那个
        Set<Tuple> chatSeenMsgSet = conn.zrangeWithScores("seen:" + recipient, 0, -1);
        /*
        获取用户在每个组中未读的消息
         */
        Transaction trans = conn.multi();
        for (Tuple seenMsgTuple : chatSeenMsgSet) {
            trans.zrangeByScore("msgs:" + seenMsgTuple.getElement(), String.valueOf(seenMsgTuple.getScore() + 1), "info");
        }
        List<Object> recWaitSeeMsgList = trans.exec();
        Iterator<Object> recWaitSeeMsgIterator = recWaitSeeMsgList.iterator();
        Iterator<Tuple> chatSeenMsgIterator = chatSeenMsgSet.iterator();
        List<ChatMessages> resultChatMessages = new ArrayList<ChatMessages>();
        List<Object[]> seenUpdates = new ArrayList<Object[]>();
        List<Object[]> msgRemoves = new ArrayList<Object[]>();
        // 每个组中已经查看的消息id最大值
        while (chatSeenMsgIterator.hasNext()) {
            Tuple chatSeenMsg = chatSeenMsgIterator.next();
            // 在每个组中未查看消息集合
            Set<String> msgSet = (Set<String>) recWaitSeeMsgIterator.next();
            if (msgSet == null || msgSet.isEmpty()) {
                continue;
            }
            List<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();
            // 未查看消息中msgId最大的
            long seenMsgMaxId = 0;
            for (String s : msgSet) {
                long msgId = Long.parseLong(MyJsonUtil.findNode(s, "messageId"));
                if (msgId > seenMsgMaxId) {
                    seenMsgMaxId = msgId;
                }
                Map<String, Object> message = MyJsonUtil.asMap(s);
                messages.add(message);
            }
            // 更新聊天组中某用户读取消息的最大id
            conn.zadd("chat:" + chatSeenMsg.getElement(), seenMsgMaxId, recipient);
            // 用户在参数的组中已读消息的最大id
            seenUpdates.add(new Object[]{"seen:" + recipient, seenMsgMaxId, chatSeenMsg.getElement()});
            // 如果组内所有人都看过该消息，则删除组对应消息的有序集合中数据
            /*
            这个算法真是精妙
             */
            Set<Tuple> tuples = conn.zrangeWithScores("chat:" + chatSeenMsg.getElement(), 0, 0);
            if (!tuples.isEmpty()) {
                msgRemoves.add(new Object[]{"msgs:" + chatSeenMsg.getElement(), tuples.iterator().next().getScore()});
            }
            resultChatMessages.add(new ChatMessages(chatSeenMsg.getElement(), messages));
        }


        trans = conn.multi();
        for (Object[] seenUpdate : seenUpdates) {
            trans.zadd((String) seenUpdate[0], (Integer) seenUpdate[1], (String) seenUpdate[2]);
        }
        for (Object[] msgRemove : msgRemoves) {
            trans.zremrangeByScore((String) msgRemove[0], 0, (Double) msgRemove[1]);
        }
        return resultChatMessages;

    }

    public class ChatMessages {
        public String chatId;
        public List<Map<String, Object>> messages;

        public ChatMessages(String chatId, List<Map<String, Object>> messages) {
            this.chatId = chatId;
            this.messages = messages;
        }

        public boolean equals(Object other) {
            if (!(other instanceof ChatMessages)) {
                return false;
            }
            ChatMessages otherCm = (ChatMessages) other;
            return chatId.equals(otherCm.chatId) &&
                    messages.equals(otherCm.messages);
        }
    }
}
