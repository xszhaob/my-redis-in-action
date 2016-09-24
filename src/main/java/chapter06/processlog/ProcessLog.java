package chapter06.processlog;

import chapter06.chat.Chat;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.*;

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


    public class CopyLogsThread extends Thread {
        private Jedis conn;
        private File path;
        private String channel;
        private int count;
        private long limit;
        private Chat chat;


        public CopyLogsThread(File path, String channel, int count, long limit) {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.path = path;
            this.channel = channel;
            this.count = count;
            this.limit = limit;
            this.chat = new Chat();
        }

        @Override
        public void run() {
            Deque<File> waiting = new ArrayDeque<File>();
            long bytesInRedis = 0;

            Set<String> recipients = new HashSet<String>();
            for (int i = 0; i < count; i++) {
                recipients.add(String.valueOf(i));
            }
            chat.createChat(conn, "source", recipients, "", channel);

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
                        conn.append((channel + logFile).getBytes(),bytes);
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

                chat.sendMessage(conn,channel,"source",logFile.toString());

                bytesInRedis += fileSize;
                waiting.add(logFile);

            }

            chat.sendMessage(conn,channel,"source",":done");

            while (waiting.size() > 0) {
                long clean = clean(waiting,count);
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


        private long clean(Deque<File> waiting, int count) {
            return 0;
        }
    }
}
