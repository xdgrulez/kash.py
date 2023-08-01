from kashpy.kash001 import *
c = Cluster("local")
c.cp("./scraped.txt", "scraped")
c.ls()
c.l()
s = '{ "type": "record", "name": "scrapedRecord", "fields": [ { "name": "datetime", "type": "string" }, { "name": "text", "type": "string" }, { "name": "source", "type": { "type": "record", "name": "sourceRecord", "fields": [ { "name": "name", "type": "string" }, { "name": "id", "type": "string" }, { "name": "user", "type": "string" } ] } } ] }'
c.cp("scraped", "scraped_avro", target_value_type="avro", target_value_schema=s)
c.l("scraped*")
c.rm("scraped")
c.l("scraped*")
c.head("scraped_avro", n=1, value_type="bytes")
c.head("scraped_avro", n=1, value_type="avro")
c.tail("scraped_avro", n=1, value_type="avro")
ppretty(c.tail("scraped_avro", n=1, value_type="avro"))
#py classify.py 
c.l("scored*")
c.l("scored*")
c.l("scored*")
c.l("scored*")
