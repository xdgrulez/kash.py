from kashpy.kash001 import *
from transformers import pipeline

classifier = pipeline('text-classification', model='ProsusAI/finbert', return_all_scores=True)

def get_fear_index(text_str):
    fear_index_int = 0
    if text_str:
        sentiment_dict_list = classifier(text_str)[0]
        for sentiment_dict in sentiment_dict_list:
            if sentiment_dict["label"] == "negative":
                fear_index_int = int(sentiment_dict["score"] * 100)
                break
    return fear_index_int

def map_function(message_dict):
    fear_index_int = get_fear_index(message_dict["value"]["text"])
    message_dict["value"]["sentiment"] = {"model": "finbert", "score": fear_index_int}
    return message_dict

c = Cluster("kafka_dev")
c.consume_timeout(-1)
schema_str = '{ "type": "record", "name": "scoredRecord", "fields": [ { "name": "datetime", "type": "string" }, { "name": "text", "type": "string" }, { "name": "source", "type": { "type": "record", "name": "sourceRecord", "fields": [ { "name": "name", "type": "string" }, { "name": "id", "type": "string" }, { "name": "user", "type": "string" } ] } }, { "name": "sentiment", "type": { "type": "record", "name": "sentimentRecord", "fields": [ { "name": "model", "type": "string" }, { "name": "score", "type": "int" } ] } } ] }'
map(c, "scraped_avro", c, "scored_avro", map_function, source_value_type="avro", target_value_type="avro", target_value_schema=schema_str)
