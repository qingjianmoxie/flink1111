package com.atguigu.flink1111.util

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object MyRedisUtil {

  val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop1").setPort(6379).build()

  def getRedisSink(): RedisSink[(String,String)] ={
    new RedisSink[(String,String)](conf,new MyRedisMapper)
  }


  class MyRedisMapper extends  RedisMapper[(String,String)]{
    //用何种命令进行保存
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET,"channel_sum")
    }
     //流中的元素哪部分是value
    override def getValueFromData(channelSum: (String, String)): String = channelSum._2
    //流中的元素哪部分是key
    override def getKeyFromData(channelSum: (String, String)): String = channelSum._1
    }
  }
