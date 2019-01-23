package cn.rumoss.redis;

import redis.clients.jedis.Jedis;

import java.util.Collections;

/**
 * @ClassName RedisLock
 * @Description Redis分布式锁
 *  分布式锁的主流实现方式：
 *  (1)基于数据库的索引和行锁，数据库乐观锁
 *  (2)基于Redis的单线程原子操作：setNX
 *  (3)基于Zookeeper的临时有序节点
 *  锁的实现需要满足下面条件：
 *  (1)互斥性，只有一个客户端能持有锁
 *  (2)无死锁，即使发生有客户端无法解锁，也能保证后续其他客户端能加锁
 * @Author HeCG
 * @Date 19-1-19 下午4:36
 */
public class RedisLock {

    public static final Long SUCCESS = 1L;
    public static final String LOCK_SUCCESS = "OK";
    public static final String SET_IF_NOT_EXIST = "NX";
    public static final String SET_WITH_EXPIRE_TIME = "PX";

    public static Jedis jedis;
    static {
        jedis = new Jedis("localhost");
    }

    /**
     * 使用 setnx 实现加锁，其中key是锁，value是锁的过期时间
     * 可能存在的问题：
     * (1)客户端自己生成过期时间，所以需要强制要求分布式下每个客户端的时间必须同步
     * (2)当锁过期的时候，如果多个客户端同时执行 getSet ，最终只有一个客户端可以加锁，但是这个客户端的锁的过期时间可能被其他客户端覆盖
     * (3)锁不具备拥有者标识，即任何客户端都可以解锁
     * @param lockName
     * @param expire
     * @return
     */
    public static boolean lock(String lockName,int expire){
        long now = System.currentTimeMillis();
        // 使用 setNx 加锁，保证操作的原子性
        boolean result = SUCCESS.equals(jedis.setnx(lockName,String.valueOf(now + expire*1000)));
        // 下面的if主要是解决死锁问题
        if(!result){
            String timestamp = jedis.get(lockName);
            // 如果设置过期时间失败的话，再通过value的时间戳来和当前时间戳比较，防止出现死锁
            if(timestamp!=null && Long.parseLong(timestamp)<now){
                // 通过 getSet 在发现锁过期未被释放的情况下，避免删除了在这个过程中有可能被其余的线程获取到了锁
                //锁已过期，获取上一个锁的过期时间，并设置现在锁的过期时间
                String oldValue = jedis.getSet(lockName,String.valueOf(now + expire*1000));
                if(oldValue!=null && oldValue.equals(timestamp)){
                    result = true;
                    jedis.expire(lockName,expire);
                }
            }
        }
        if(result){
            jedis.expire(lockName,expire);
        }
        return result;
    }

    /**
     * 更好的一种加锁方法
     * @param lockName
     * @param expire
     * @param requestId
     * @return
     */
    public static boolean lockBetter(String lockName,int expire,String requestId){
        // "NX" 当key不存在时，我们进行set操作；若key已经存在，则不做任何操作
        //"PX" 给key加一个过期时间
        String result = jedis.set(lockName, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expire*1000);
        if(LOCK_SUCCESS.equals(result)){
            return true;
        }
        return false;
    }

    /**
     * 解锁
     * 为什么使用Lua脚本？
     * 保证操作的原子性。eval命令执行Lua代码的时候，Lua代码将被当成一个命令去执行，并且直到eval命令执行完成，Redis才会执行其他命令。
     * Redisson分布式锁就是使用的Lua脚本
     * @param lockName
     * @param requestId
     * @return
     */
    public static boolean unLock(String lockName,String requestId){
        // KEYS[1] lockName 锁名称 ARGV[1] requestId
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object result = jedis.eval(script, Collections.singletonList(lockName), Collections.singletonList(requestId));
        if (SUCCESS.equals(result)) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {

        // 标识哪个请求加的锁
        String lockName = "foo_bar";
        String requestId = "003";
        int expire = 30;
        boolean result = lockBetter(lockName,expire,requestId);
        //boolean result = lock(lockName,expire);
        System.out.println("lock result: " + result);
        result = lockBetter(lockName,expire,requestId);
        //result = lock(lockName,expire);
        System.out.println("lock again result: " + result);
        result = unLock(lockName,requestId);
        System.out.println("unlock result: " + result);
        result = lockBetter(lockName,expire,requestId);
        System.out.println("lock result: " + result);
        result = lockBetter(lockName,expire,requestId);
        System.out.println("lock again result: " + result);

    }

}
