package chapter09;

import java.util.zip.CRC32;

/**
 * Created by zhaobo on 2016/11/14.
 */
public class ShardTest {

    public static void main(String[] args) {

    }

    public String shardKey(String base,String key,long totalElements,int shardSize) {
        long shardId = 0;
        if (isDigit(key)) {
            shardId = Integer.parseInt(key,10) / shardSize;
        } else {
            // 对于不是整数的键，shard_key方法将计算出它们的CRC32校验和。
            CRC32 crc = new CRC32();
            crc.update(key.getBytes());
            long shards = 2 * totalElements / shardSize;
            shardId = Math.abs((int) crc.getValue() % shards);
        }
        return base + ":" + shardId;
    }


    private boolean isDigit(String str) {
        for (char c : str.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }
}
