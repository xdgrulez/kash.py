* touch mit Liste

* lags?

* changes:
  * config_dict bei set_config
  
* set offsets (? - do we want to support assignments without consumer groups at all?)

* Transactional API!!!

* subscribe mit mehreren Topics (Pattern auch???)

* produce mit Schema-ID (nicht ganzes Schema) - ist jetzt in produce() implementiert, noch nachziehen + tests anpassen

* consumer_timeout() dokumentieren!!

* set_broker_config und set_config mit Records statt nur einzeln

* clusters OK
* neue Consumer Group-Features OK
* Pretty print JSON (indent) OK
* LakeFS ausprobieren; evtl. auch Versionierungs-Layer für kash.py OK (ist kein Git, sondern eher CVS)
* kash.py-Layering/Modularisierung
  - original wrapper
  - functional layer? vielleicht core
  - file layer? vielleicht core
  - table layer
  - versioning layer
  - blob storage layer
  - pandas Layer (u.a. Topic2Table/Table2Topic, CSV, XLSX...)
* Argument-Variablen (auch obligatorische Argumente) ohne Typannotationen
* Tests: alle Consumer Groups explizit erzeugen und aufräumen nach jedem Test
* Schema Registry-Funktionen von confluent_kafka einbauen
* Default für value_type in kash-Section
* REST-Proxy-Support

# Methods

## AdminClient

* watermarks
* list_topics
* config
* set_config
* create/touch
* delete/rm
* offsets_for_times (cluster-only)
* exists
* partitions (=> no more describe but partitions(verbose))
* set_partitions (cluster-only)


## RestProxy
