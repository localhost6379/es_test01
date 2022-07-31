# 查看索引
GET /_cat/indices/?v

# 删除索引 
DELETE /product 

# 创建索引并创建映射
PUT /product
{
  "mappings": {
    "properties": {
      "title":{
        "type": "keyword"
      },
      "price":{
        "type": "double"
      },
      "create_time":{
        "type": "date"
      },
      "desc":{
        "type": "text",
        "analyzer": "ik_max_word"
      }
    }
  }
}

# 查看映射
GET /index01/_mapping

# 查看数据
GET /index01/_search
{"query":{"match_all":{}}}

