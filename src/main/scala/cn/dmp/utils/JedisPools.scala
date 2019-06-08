package cn.dmp.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object JedisPools {

//  private val jedisPool = new JedisPool(new GenericObjectPoolConfig, "localhost", 6379)

  //存储到8号库
  private val jedisPool = new JedisPool(new GenericObjectPoolConfig, "localhost", 6379, 3000, null, 8)

  def getJedis() = jedisPool.getResource

}
