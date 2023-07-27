* type statt value_type und key_type falls beides ein Format hat
* Disclaimer bei r.l() bei compacted topics...
* ppretty zugänglich machen (ohne "from kashpy.helpers import *")
* offsets mit REST Proxy?
* key_type/value_type vorkonfigurieren (für head/tail...)
* Präfix für generierte Consumer-Group-Namen (für Authorisierung)

* produce n for all
* recreate!
* wc, grep, diff (shell.py)/zip (functional.py)
* local S3 - dann s3/s3_reader, s3/s3_writer s3/s3
* tests: functional -> shell -> storage -> filesystem -> local, s3
                                    kafka -> cluster, restproxy
* next talk: keywords on slides, no (almost) full sentences
* screenshots: use Jupyter Notebook instead of Terminal


* intra-cluster map: support source/target-key/value types + schemas
* Pretty print JSON etc. (as default output?)
* kash.py layering:
  - connection -> kafka -> cluster/restproxy etc.
  - connection -> file -> local/blob
  - functional layer
  - table layer?
  - versioning layer?
  - pandas/Bytewax/River?
* all arguments (also obligatory ones) without "hungarian" type annotations
* tests: create consumer groups explicitly and clean them up
* schema registry functionality?
* set default value for value_type in kash-section

# Methods

## AdminClient

### Topics

* size() (kafka)
* topics()/ls()/l()/ll() (kafka)
* exists() (kafka)

* watermarks()
* list_topics()
* config()
* set_config() - (TODO: allow records, not just one key/value pair)
* create()/touch() - (TOOD: support replication factor, support list of topics to be created)
* delete()/rm()
* offsets_for_times()
* X describe()
* partitions()
* set_partitions() (cluster-only)

### Consumer Groups

* groups()
* describe_groups()
* delete_groups() (cluster-only)
* group_offsets()
* alter_group_offsets()

### Brokers

* brokers()
* broker_config()
* set_broker_config() - (TODO: allow records, not just one key/value pair)

### ACLs

* acls()
* create_acl()
* delete_acl()

## Producer

* produce() - (TODO: provide schema ID only/no schema)
* flush() (cluster-only)
* purge() (TODO)
* abort_transaction() (TODO)
* begin_transaction() (TODO)
* commit_transaction() (TODO)
* init_transaction() (TODO)
* poll() (TODO)
* send_offsets_to_transaction() (TODO)

better:
* producer() - create producer
* producers() - list producers
* => default: only one producer per e.g. cluster object as in kash.py before

## Consumer

* subscribe() - (TODO: support for multiple topics)
* unsubscribe()
* consume() - (TODO: document consumer_timeout())
* commit()
* offsets()
* close()
* memberid() (cluster-only)
* assign() (TODO?)
* assignment() (TODO?)
* committed() (TODO?)
* incremental_assign() (TODO?)
* incremental_unassign() (TODO?)
* pause() (TODO?)
* position() (TODO?)
* resume() (TODO?)
* seek() (TODO?)
* store_offsets() (TODO?)
* unassign() (TODO?)

cluster:
* subs() (kafka)
* create_subscription_id() (kafka)
* get_subscription_id() (kafka)
* get_subscription() (kafka)

better:
* consumer()
* consumers()
* -> close()
* subscribe()
* subscriptions()/subs()
* -> unsubscribe()
* => default: only one consumer + one subscription per e.g. cluster object as in kash.py before
