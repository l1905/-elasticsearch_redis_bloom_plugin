import random
import base64
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch()

body = {
    "mappings": {
        "image_search": {
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "user_id": {
                    "type": "keyword"
                },
                "score": {
                    "type": "keyword"
                }
            }
        }
    }
}

index = 'test'
es.indices.delete(index=index, ignore=404)
es.indices.create(index=index, ignore=400, body=body)



def generator():
    i = 0
    while True:
        yield {

                'id': i,
                "user_id": i%5,
                "score": i,
            }
        i += 1
        if i >= 100:
            break


if __name__ == "__main__":
    # 批量插入100w数据到es
    result, status = helpers.bulk(es, generator(), index=index)
    print(result)
    print(status)