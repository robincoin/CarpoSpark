# CarpoSpark
从Json配置里面读取并创建Spark的DAG流程图，不用在根据不同的需求，开发不同的Spark程序。
目前支持的业务有，指定HDFS文件目录，过滤行，过滤列。Join操作，Union操作，Group分组汇聚操作（max,min,count,sum,avg）。
可自定义输出目录和文件格式。