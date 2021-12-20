from elasticsearch import Elasticsearch

es = Elasticsearch()

body = {
    "from": 0,
    "size": 8,
    "_source": {
        "excludes": ""
    },
    "sort": {
        "_score": {
            "order": "desc"
        }
    },
    "query": {
        "function_score": {
            "query":{
                "bool":{
                    # 这里不能用filter，否则分数不生效
                    "must": [
                        { "range": { "article_id": { "gte": "80" }}}
                    ],
                },
            },
            "functions": [
                {
                    "script_score": {
                        "script": {
                            "source": "BloomFilterRecore",
                            "lang": "BloomFilterRecore",
                            "params": {
                                "field": "test",
                                "user_id": "25855",
                                "device_id": "123456",
                            }
                        }
                    },

                }
            ]
        }
    }
}

if __name__ == "__main__":
    time_list = list()
    result = es.search(index='test', body=body)
    for hit in result['hits']['hits']:
        print(hit)
