package com.lastproject.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisUtil {

  //连接redis的工具

  private val jedisPool = new JedisPool(new GenericObjectPoolConfig, "localhost", 6379, 30000, null, 1)

  def getJedis = jedisPool.getResource

}
