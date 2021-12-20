import random
import base64
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch()

body = {
    "mappings": {
        "properties": {
            "id": {
                "type": "Numbers"
            },
            "article_id": {
                "type": "Numbers"
            },
            "rating_num": {
                "type": "Numbers"
            },
            "comment_num": {
                "type": "Numbers"
            },

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
                "article_id": i,
                "rating_num": i*10,
                "comment_num": i*2,
            }
        i += 1
        if i >= 100:
            break


if __name__ == "__main__":
    # 批量插入100w数据到es
    result, status = helpers.bulk(es, generator(), index=index)
    print(result)
    print(status)