# RedC
RedC (stands for Redis Connector) is a wrapper around Redis::Fast that provides several useful possibilities:
1. Connection "naming": RedC->new(name => 'some_objects', index => 3)
2. Auto-selecting of desired database number on connect and associate this number with connection name
3. Auto-switching to selected database number on reconnection
4. New methods: write and read to do mset/mget with tranparent (de)serialization using Tag::DeCoder package
5. New methods: databases - to get maximum number of databases allowed for this (local) Redis instance

Limitations:
For now it works only with the Redis server running localy, because databases() method simply parses Redis configuration file

