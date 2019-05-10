package com.atguigu.flink1111.util

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import javax.sql.DataSource

import com.alibaba.druid.pool.DruidDataSourceFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class MyjdbcSink(sql:String)  extends  RichSinkFunction[Array[Any]]{

  val driver="com.mysql.jdbc.Driver"

  val url="jdbc:mysql://hadoop2:3306/gmall1111?useSSL=false"

  val username="root"

  val password="123123"

  val maxActive="20"

  var connection:Connection=null;


 // 创建连接
  override def open(parameters: Configuration): Unit = {

    val properties = new Properties()

    properties.put("driverClassName",driver)
    properties.put("url",url)
    properties.put("username",username)
    properties.put("password",password)
    properties.put("maxActive",maxActive)


    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
    connection= dataSource.getConnection
  }

  // 把每个Array[Any] 作为数据库表的一行记录进行保存
  override def invoke(values: Array[Any]): Unit = {
    val ps: PreparedStatement = connection.prepareStatement(sql)  // insert into xxx values (?,?,?)
    for (   i <- 0 to values.length-1    ) {
      ps.setObject(i+1 ,values(i))
    }
    ps.executeUpdate()

  }
  //关闭连接
  override def close(): Unit = {
     if(connection!=null){
       connection.close()
     }

  }




}
