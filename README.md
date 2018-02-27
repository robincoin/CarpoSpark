# CarpoSpark
从Json配置里面读取并创建Spark的DAG流程图，不用再根据不同的需求，开发不同的Spark程序。
目前支持的业务有，指定HDFS文件目录，过滤行，过滤列。Join操作，Union操作，Group分组汇聚操作（max,min,count,sum,avg）。
可自定义输出目录和文件格式。
格式如下

{
  "id": "spark_00002",
  "name": "Union过滤",
  "size": 10,
  "postfix": "test",
  "suffix": "",
  "extension": "csv",
  "format": "",
  "output": "/data/spark/testout",
  "split": "|",
  "nodes": {
    "node_11": {
      "input": "/data/grid/*.csv",
      "type": "input"
    },
    "node_12": {
      "type": "filter_col",
      "split": ",",
      "fields": {
        "time": {
          "name": "time",
          "text": "time",
          "idx": "0"
        },
        "type": {
          "name": "type",
          "text": "type",
          "idx": "1"
        }
      }
    },
    "node_13": {
      "type": "map",
      "split": ",",
      "key_col": -1
    },
    "node_21": {
      "input": "/data/num/*.csv",
      "type": "input"
    },
    "node_22": {
      "type": "filter_col",
      "split": ",",
      "fields": {
        "time": {
          "name": "time",
          "text": "time",
          "idx": "0"
        },
        "type": {
          "name": "type",
          "text": "type",
          "idx": "1"
        }
      }
    },
    "node_23": {
      "type": "map",
      "split": ",",
      "key_col": -1
    },
    "node_8": {
      "type": "distinct"
    },
    "node_6": {
      "type": "output",
      "time_col": "0",
      "time_format1": "yyyyMMdd",
      "time_format2": "yyyy",
      "split": ",",
      "fields": {
        "time": {
          "name": "time",
          "text": "time",
          "idx": "0"
        },
        "type": {
          "name": "type",
          "text": "type",
          "idx": "1"
        }
      }
    },
    "node_1": {
      "type": "union"
    }
  },
  "lines": {
    "line_4": {
      "inputs": "node_21",
      "outputs": "node_22"
    },
    "line_5": {
      "inputs": "node_22",
      "outputs": "node_23"
    },
    "line_6": {
      "inputs": "node_23",
      "outputs": "node_1"
    },
    "line_1": {
      "inputs": "node_11",
      "outputs": "node_12"
    },
    "line_2": {
      "inputs": "node_12",
      "outputs": "node_13"
    },
    "line_3": {
      "inputs": "node_13",
      "outputs": "node_1"
    },
    "line_7": {
      "inputs": "node_1",
      "outputs": "node_8"
    },
    "line_8": {
      "inputs": "node_8",
      "outputs": "node_6"
    }
  }
}