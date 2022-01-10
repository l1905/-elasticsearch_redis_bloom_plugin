



一、背景


推荐系统使用ES作为召回池， 在ES侧做召回查询时， 如果直接过滤掉用户已读文章， 可做到精准召回，提高推荐召回准确率， 降低推荐系统运算压力

二、问题拆解


直接过滤掉用户已读文章， 可理解为 将用户已读的文章 打分为0，按打分倒序，优先展示打分不为0，即用户未读文章。

查询出结果后， 过滤掉打分为0的文章，剩余文章即我们已过滤用户已经的文章。



三、ES插件编写


具体demo方式实现见： https://github.com/l1905/-elasticsearch_redis_bloom_plugin



我们查看官网文档， 编写ES插件需要遵循以下规范

```
# Elasticsearch plugin descriptor file
# This file must exist as 'plugin-descriptor.properties' inside a plugin.
#
### example plugin for "foo" 编写一个叫做foo的插件
#
# foo.zip <-- zip file for the plugin, with this structure， zip格式的插件， 有具体以下内容:
# |____   <arbitrary name1>.jar <-- classes, resources, dependencies， 依赖的jar包
# |____   <arbitrary nameN>.jar <-- any number of jars， 其他以来的jar包
# |____   plugin-descriptor.properties <-- 插件描述文件  example contents below:
#
# classname=foo.bar.BazPlugin  插件的入口类名称
# description=My cool plugin  插件名称
# version=6.0  插件版本
# elasticsearch.version=6.0 依赖的ES版本
# java.version=1.8 版本
#
### mandatory elements for all plugins:
#
# 'description': simple summary of the plugin
description=${description}
#
# 'version': plugin's version
version=${version}
#
# 'name': the plugin name
name=${name}
#
# 'classname': the name of the class to load, fully-qualified.
classname=${classname}
#
# 'java.version': version of java the code is built against
# use the system property java.specification.version
# version string must be a sequence of nonnegative decimal integers
# separated by "."'s and may have leading zeros
java.version=${javaVersion}
#
# 'elasticsearch.version': version of elasticsearch compiled against
elasticsearch.version=${elasticsearchVersion}
```


我们看到官方例子(https://github.com/elastic/elasticsearch/tree/master/plugins/examples)

采用gradle方式打包， 但我们基于开发环境， 采用Maven方式打包。



如果采用Maven方式打包成zip文件， 一般使用<maven-assembly-plugin>插件， 该插件和 <maven-shade-plugin>区别，主要在于

maven-assembly-plugin:  提取所有的依赖jar包，适用于依赖较少情况，如果依赖较多，可能会有命名冲突
<maven-shade-plugin>： 将所有依赖打包到1个jar包中， jar包变成可执行包。


因此我们采用<maven-assembly-plugin>方式进行包， 且需要对打包方式进行配置， 即需要将哪些包，哪些文件打包进去， 我们采用自定义配置方式



src/main/assemblies/plugin.xml

```xml
<?xml version="1.0"?>
<assembly>
    <id>plugin</id>
    <formats>
        <format>zip</format>
    </formats>
    <includeBaseDirectory>false</includeBaseDirectory>

    <files>
        <file>
            <source>${project.basedir}/src/main/resources/plugin-descriptor.properties</source>
            <outputDirectory></outputDirectory>
            <filtered>true</filtered>
        </file>
        <file>
            <source>${project.basedir}/src/main/resources/plugin-security.policy</source>
            <outputDirectory></outputDirectory>
            <filtered>true</filtered>
        </file>
    </files>
    <dependencySets>
        <dependencySet>
            <outputDirectory>/</outputDirectory>
            <useProjectArtifact>true</useProjectArtifact>
            <useTransitiveFiltering>true</useTransitiveFiltering>
            <excludes>
                <exclude>org.elasticsearch:elasticsearch</exclude>
            </excludes>
        </dependencySet>

    </dependencySets>
</assembly>
```



这里我们可以看到， 其中将plugin-security.policy文件打包进去，该文件是说需要赋予该插件哪些权限访问， 具体配置规则如下：

```
grant {
// needed because of problems in unbound LDAP library
permission java.util.PropertyPermission "*", "read,write";

      // classloader
      permission java.lang.RuntimePermission "setContextClassLoader";
      permission java.lang.RuntimePermission "getClassLoader";

      // socket
      permission java.net.SocketPermission "*", "connect,resolve";

      permission javax.management.MBeanServerPermission "createMBeanServer";
      permission javax.management.MBeanServerPermission "findMBeanServer";
      permission javax.management.MBeanPermission "org.apache.commons.pool2.impl.GenericObjectPool#-[org.apache.commons.pool2:name=pool,type=GenericObjectPool]", "registerMBean";
      // Allow MBeanTrustPermission register
      //permission javax.management.MBeanTrustPermission "register";
};
```



这里配置相关我们已准备好， 具体的插件模式实现，我们参考官网的examples/script-expert-scoring 的实现方式， 该插件需要继承 Plugin类，实现ScriptPlugin插件。 具体实现逻辑如下



在ES启动时， 初始化Redis连接池。
在ES启动时，初始化本地缓存<userId, bloom对象> 采用Guava缓存，如果多路召回， 可以复用该用户的Bloom对象
在用户进行查询时，获取用户的bloom对象，即用户一次DSL查询，只获取一次bloom对象
在查询过程中， 计算召回的每一篇文章的分数时， 判断文章ID是否在bloom对象中，如果在， 分数置零，如果不在， 保持原分数
查询按照分数排序，获取前N行数据


具体代码可以看demo， 部署方案为：



1. 打包代码 mvn clean package，在target目录会生成Bloomfilter目录

2. 目前本地采用docker部署ES

    下载：docker pull docker.elastic.co/elasticsearch/elasticsearch:7.16.1  
    单机运行：docker run -p 127.0.0.1:9200:9200 -p 127.0.0.1:9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.16.1  
    重启运行: docker restart e0070a343764

3. 进入到target目录，将zip目录复制到ES容器中
   docker cp Bloomfilter-plugin.zip e0070a343764:/usr/share/elasticsearch

4. 进入docker命令行环境
   docker exec -it e0070a343764 /bin/bash
   默认会进入到 /usr/share/elasticsearch目录

5. 查看是否已安装插件， 如果安装，则卸载已安装的bloomfilter插件
   elasticsearch-plugin list
   elasticsearch-plugin remove Bloomfilter

6. 进行安装插件
   elasticsearch-plugin install file:///usr/share/elasticsearch/Bloomfilter-plugin.zip
   会提示是否授权该插件哪些权限，直接授权即可

7. 需要退出容器，进行容器重启
   docker restart e0070a343764

8. 打开docker日志，监控ES是否报错
   docker logs -f e0070a343764


   

demo代码中会有对应的python测试脚本，可用测试脚本插入，查询数据，做验证。


可能存在的问题：



ES集群存在多台node服务器，则一次用户查询，会去多台机器上查询，则每台机器上都会做一次Redis查询，会放大Redis查询次数
如果DSL查询召回文章较多，比如几万条， 则需要对每篇文章做判断，是否用户已读， 存在过多计算，




参考文档：

https://zhuanlan.zhihu.com/p/141854299
官方文档： https://www.elastic.co/guide/en/elasticsearch/plugins/7.16/intro.html
https://www.cnblogs.com/whb-20160329/p/10472717.html
bitmaps实现过滤 https://github.com/yzlq99/ElasticsearchPlugin
https://medium.com/tinder-engineering/how-we-improved-our-performance-using-elasticsearch-plugins-part-2-b051da2ee85b
提问说遇到性能问题：https://stackoverflow.com/questions/67798622/score-script-plugin-in-elasticsearch
权限设置：https://blog.csdn.net/goodluck_mh/article/details/92845023
权限设置：https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-authors.html#plugin-authors-jsm
权限设置：https://blog.csdn.net/dxy3166216191/article/details/108084644
参考权限设置：https://github.com/KennFalcon/elasticsearch-analysis-hanlp
权限设置参考：https://www.pianshen.com/article/9700823457/
官方新的pr-MBeanTrustPermission：https://github.com/elastic/elasticsearch/pull/81508
其他参考：http://www.4k8k.xyz/article/qq_38680405/107225724


