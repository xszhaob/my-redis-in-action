package chapter06.processlog;

import chapter06.chat.Chat;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/24
 * <p/>
 * package_name:chapter06.processlog
 * <p/>
 * project: my-redis-in-action
 */
public class ProcessLog {

    private Chat chat = new Chat();


    public static void main(String[] args) throws IOException, InterruptedException {
        File file = new File("E:\\temp");
        ProcessLog processLog = new ProcessLog();
        processLog.new CopyLogsThread(file,"bo.zhao",5,60);
        processLog.processLogsFromRedis(new Jedis(), "1", new CallBack() {
            public void callBack(String line) {
                System.out.println(line);
            }
        });
    }

    /**
     * 给定文件处理器处理日志文件
     *
     * @param conn     redis连接
     * @param id       文件处理器id
     * @param callBack 回调函数
     * @throws IOException
     * @throws InterruptedException
     */
    public void processLogsFromRedis(Jedis conn, String id, CallBack callBack) throws IOException, InterruptedException {
        while (true) {
            // id用户获取待处理的日志文件
            List<Chat.ChatMessages> chatMessages = chat.fetchPendingMessage(conn, id);
            // 处理日志
            for (Chat.ChatMessages chatMessage : chatMessages) {
                for (Map<String, Object> message : chatMessage.messages) {
                    String logFile = (String) message.get("message");
                    // 如果文件名称是:done，则认为已经处理完毕
                    if (":done".equals(logFile)) {
                        return;
                    }
                    if (logFile == null || logFile.isEmpty()) {
                        continue;
                    }

                    // 处理文件
                    InputStream in = new RedisInputStream(conn, chatMessage.chatId + logFile);
                    if (logFile.endsWith(".gz")) {
                        in = new GZIPInputStream(in);
                    }

                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    try {
                        String readLine;
                        while ((readLine = reader.readLine()) != null) {
                            callBack.callBack(readLine);
                        }
                        callBack.callBack(null);
                    } finally {
                        reader.close();
                    }

                    // 处理完成之后，该文件已处理次数+1
                    conn.incr(chatMessage.chatId + logFile + ":done");
                }
            }

            // 空文件，线程睡眠0.1秒
            if (chatMessages.isEmpty()) {
                Thread.sleep(100);
            }
        }
    }

    /**
     * 回调接口
     */
    public interface CallBack {
        void callBack(String line);
    }

    /**
     * 以redis数据作为数据源的数据流读取类
     */
    public class RedisInputStream extends InputStream {
        private Jedis conn;
        private String key;
        private int pos;

        public RedisInputStream(Jedis conn, String key) {
            this.conn = conn;
            this.key = key;
        }

        @Override
        public int read() throws IOException {
            byte[] subStr = conn.substr(key.getBytes(), pos, pos);
            if (subStr == null || subStr.length == 0) {
                return -1;
            }
            pos++;
            return subStr[0] & 0xff;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            // TODO: 2016/10/9 这个地方是否有问题？
            byte[] block = conn.substr(key.getBytes(), pos, pos + (len - off - 1));
            if (block == null || block.length == 0) {
                return -1;
            }
            System.arraycopy(block, 0, buf, off, block.length);
            pos += block.length;
            return block.length;
        }

        @Override
        public int available() throws IOException {
            long len = conn.strlen(key);
            return (int) (len - pos);
        }

        /**
         * 重写close方法，不需要关闭文件
         */
        @Override
        public void close() {

        }
    }


    /**
     * 将给定文件存储到redis中
     */
    public class CopyLogsThread extends Thread {
        private Jedis conn;
        private File path;
        private String channel;
        private int count;
        private long limit;


        public CopyLogsThread(File path, String channel, int count, long limit) {
            this.conn = new Jedis();
            this.path = path;
            this.channel = channel;
            this.count = count;
            this.limit = limit;
        }

        @Override
        public void run() {
            Deque<File> waiting = new ArrayDeque<File>();
            long bytesInRedis = 0;

            Set<String> recipients = new HashSet<String>();
            for (int i = 0; i < count; i++) {
                recipients.add(String.valueOf(i));
            }
            // conn sender 聊天组成员 信息 chatId
            chat.createChat(conn, "source", recipients, "", channel);

            // FilenameFilter 实现此接口的类实例可用于过滤器文件名
            File[] files = path.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.startsWith("temp_redis");
                }
            });
            Arrays.sort(files);
            for (File logFile : files) {
                long fileSize = logFile.length();

                while (bytesInRedis + fileSize > limit) {
                    long cleaned = clean(waiting, count);
                    if (cleaned > 0) {
                        bytesInRedis -= cleaned;
                    } else {
                        try {
                            sleep(250);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }


                BufferedInputStream bis = null;
                try {
                    bis = new BufferedInputStream(new FileInputStream(logFile));
                    int readBytes;
                    byte[] buffer = new byte[1024 * 8];
                    while ((readBytes = bis.read(buffer, 0, buffer.length)) > 0) {
                        byte[] bytes = buffer;
                        if (buffer.length != readBytes) {
                            bytes = new byte[readBytes];
                            System.arraycopy(buffer, 0, bytes, 0, readBytes);
                        }
                        conn.append((channel + logFile).getBytes(), bytes);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (bis != null) {
                        try {
                            bis.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

                // 提醒监听者，文件已准备就绪
                chat.sendMessage(conn, channel, "source", logFile.toString());

                bytesInRedis += fileSize;
                waiting.add(logFile);

            }

            // 所有文件已经处理完毕，向监听者报告此事
            chat.sendMessage(conn, channel, "source", ":done");

            // 工作完成后，清理无用的日志文件
            while (waiting.size() > 0) {
                long clean = clean(waiting, count);
                if (clean > 0) {
                    bytesInRedis -= clean;
                } else {
                    try {
                        sleep(250);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * 清理日志文件
         *
         * @param waiting 待清理的日志文件
         * @param count   日志处理客户端数量
         * @return 清理的日志文件大小
         */
        private long clean(Deque<File> waiting, int count) {
            if (waiting == null || waiting.isEmpty()) {
                return 0;
            }
            File firstFile = waiting.getFirst();
            if (String.valueOf(count).equals(conn.get(channel + firstFile + ":done"))) {
                conn.del(channel + firstFile, channel + firstFile + ":done");
                return waiting.removeFirst().length();
            }
            return 0;
        }
    }
}
