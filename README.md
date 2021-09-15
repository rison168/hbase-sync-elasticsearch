
### Hbase数据同步到ElasticSearch(协作器Coprocessor方案)

* 打包，将包上传到hdfs，并修改权限

  ~~~shell
  hadoop fs -put hbase-sync-elasticsearch-1.0-SNAPSHOT.jar /hbase_es
  hadoop fs -chmod -R 777 /hbase_es
  ~~~

* 进入hbase shell（这里采用单表替换，比较灵活）

  ~~~mysql
  # 建表
  create 'test_record','info'
  # 停用表
  disable 'test_record'
  # 替换协作器（coprocessor）,这里要小心，多一个空格都不可以
  alter 'test_record', METHOD => 'table_att', 'coprocessor' => 'hdfs://hbase_es/hbase-sync-elasticsearch-1.0-SNAPSHOT.jar|HbaseDataSyncEsObserver|1001|es_cluster=zcits,es_type=zcestestrecord,es_index=zcestestrecord,es_port=9100,es_host=master'
  # 启用表
  enable 'test_record'
  
  ### 操作参数备注
  coprocessor对应的格式以|分隔，依次为：
  - jar包的HDFS路径
  - Observer的主类
  - 优先级（一般不用改）
  - 参数（一般不用改）
  - 新安装的coprocessor会自动生成名称：coprocessor + $ + 序号（通过describe table_name可查看）
  
  
  ~~~

* 结束

  如果成功，就会显示用时多少。

  如果替换的程序有问题或者语句有问题，这一步会造成集群挂掉，如果集群挂掉，Hbase是无法重启的。需要进行如下步骤：

  在hbase-site.xml 文件

  ~~~xml
  # 添加
  hbase.coprocessor.abortonerror false 
  hbase.table.sanity.checks false
  <property>
      <name>hbase.coprocessor.abortonerror</name>
      <value>false</value>
   </property>
  ~~~

  报错会被抑制，解绑协作处理器，然后重启。

  ~~~mysql
  #以后对jar包内容做了调整，需要重新打包并绑定新jar包，再绑定之前需要做目标表做解绑操作，加入目标表之前绑定了同步组件的话，以下是解绑的命令
  disable 'test_record'
  alter 'test_record', METHOD => 'table_att_unset',NAME => 'coprocessor$1'
  enable 'test_record'
  desc 'test_record'
  # desc后查看没有协处理器了就是卸载了
  # 两个配置删除后再重启hbase
  ~~~

  注意点：

  绑定之后如果在执行的过程中有报错或者同步不过去，可以到hbase的从节点上的logs目录下查看regionserver报错信息,因为协作器是部署在regionserver上的，所以要到从节点上面去看日志，而不是master节点。

  切记:一个hbase表对应一个协处理器,就是说在两个表就要上传两次协处理器jar包,路径不能相同

  如果报错,卸载协处理,然后再次上传的时候,路径一定不能相同,相同可能替换不成功,也就是说,如果路径相同,他可能用的还是卸载的那个协处理器

  还有一个问题解绑后请将上边两个配置删除在重启hbase
  如果上边两个参数在,即使协处理器错误,也不会报错,相当于没有反应,所以在测试阶段请将上边两个参数去掉,生产阶段打开。

  这两个参数添加后会抑制报警,即使出错也不会报警,不会使hbase因为替换协处理器而挂掉,如果加上这个参数,替换协处理器后数据写入不进去,多半是程序,或者替换语句有问题,尤其是替换语句,非常严格,空格都不能多。

  还有一个问题解绑后请将上边两个配置删除在重启hbase
  如果上边两个参数在,即使协处理器错误,也不会报错,相当于没有反应,所以在测试阶段请将上边两个参数去掉,生产阶段打开。
  

