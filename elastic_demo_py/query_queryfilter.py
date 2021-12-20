from elasticsearch import Elasticsearch

es = Elasticsearch()

body = {
    "from": 0,
    "size": 8,
    "_source": {
        "excludes": ""
    },
    "sort": {
        "article_id": {
            "order": "desc"
        }
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "script": {
                        "script": {
                            "source": "BloomFilterQueryFilter",
                            "lang": "BloomFilterQueryFilter",
                            "params": {
                                "field": "test",
                                "user_id": "25855",
                                "device_id": "123456",
                            }
                        }
                    },
                },
                {
                    "range": {
                        "article_id":{
                            "gte":"80",
                        }
                    }
                }

           ],
            # "must":{
            #     "range": {"article_id":{
            #         "gte":"80",
            #     }}
            # }
        }
    }
}

if __name__ == "__main__":
    time_list = list()
    result = es.search(index='test', body=body)
    for hit in result['hits']['hits']:
        print(hit)
