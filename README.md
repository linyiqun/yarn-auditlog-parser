# yarn-auditlog-parser
Yarn的hdfs-audit.log的日志文件解析，从ip,用户名，时间段维度对hdfs的qps量进行统计

## 原理介绍
此工具用于分析hdfs-audit日志文件中的hdfs请求,比如下面是一条完整的记录
```
2015-09-09 05:29:54,727 INFO FSNamesystem.audit: allowed=true	ugi=data (auth:SIMPLE)	ip=/192.128.10.15	cmd=open	
src=/user/data/.staging/job_1441718170682_2949/job.jar	dst=null	perm=null	proto=rpc
```
解析程序主要抽取出其中的时间,ugi用户信息,ip地址信息,cmd操作命令信息,然后进行各个维度统计.

## 下面是几种使用方法
### 1.用户分时段统计
```
java -jar parse.jar /your-hdfs-audit-dir username
```

### 2.ip分时段统计
```
java -jar parse.jar /your-hdfs-audit-dir ip
```

### 3.qps分时段统计
```
java -jar parse.jar /your-hdfs-audit-dir qps 1(timeInternal)
```

### 4.指定用户分时段统计
```
java -jar parse.jar /your-hdfs-audit-dir username exampleName
```

### 5.指定ip分时段统计
```
java -jar parse.jar /your-hdfs-audit-dir ip exampleIp
```
