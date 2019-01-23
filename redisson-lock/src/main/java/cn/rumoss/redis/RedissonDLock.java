package cn.rumoss.redis;


import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName RedissonDLock
 * @Description 使用Redisson通过的分布式锁
 * @Author HeCG
 * @Date 19-1-22 上午12:44
 */
public class RedissonDLock {

    public static RedissonClient client;
    static {
        Config config = new Config();
        //String address = "redis://127.0.0.1:6379";
        String address = "127.0.0.1:6379";
        config.useSingleServer().setAddress(address);
        client = Redisson.create(config);
    }

    public static boolean lock(String lockName,int expire){
        RLock dlock = client.getLock(lockName);
        boolean result = false;
        try {
            // expire 等待时间 leaseTime 超时时间
            result = dlock.tryLock(expire, 20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (dlock.isLocked()) {
                dlock.unlock();
            }
        }
        return result;
    }

    public static void main(String[] args) {

        // 标识哪个请求加的锁
        String lockName = "foo_bar";
        int expire = 30;
        boolean result = lock(lockName,expire);
        System.out.println("lock result: " + result);
        result = lock(lockName,expire);
        System.out.println("lock again result: " + result);

    }

}
