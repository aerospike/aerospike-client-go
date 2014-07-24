# Data Model

<!--
################################################################################
binmap
################################################################################
-->
<a name="binmap"></a>

## BinMap

BinMap is a type defined as map[string]interface{} to facilitate declaring bin data.

```go
  bins := BinMap{
    "name"          : "Abu Rayhan Biruni",
    "contribution"  : "accurately calculated the radious of earth in 11th century",
    "citation"      : "https://en.wikipedia.org/wiki/History_of_geodesy#Biruni",
  }
```

<!--
################################################################################
record
################################################################################
-->
<a name="record"></a>

## Record

A record is how the data is represented and stored in the database. A record is represented as a `struct`.

Fields are:
- `Bins` — Bins and their values are represented as a BinMap (map[string]interface{})
- `Duplicates` — If the writepolicy.GenerationPolicy is DUPLICATE, it will contain older versions of bin data
- `Expiration` — TimeToLive of the record in seconds. Shows in how many seconds the data will be erased if not updated.
- `Generation` — Record generation (number of times the record has been updated).

The keys of the Bins are the names of the fields (bins) of a record. The values for each field can either be u/int/8,16,32,64, string, Array or Map.

Note: Arrays and Maps can contain an array or a map as a value in them. In other words, nesting of complex values is allowed.

Records are returned as a result of `Get` operations. To write back their values, one needs to pass their Bins field to the `Put` method.

Simple example of a Read, Change, Update operation:

```go
  // define a client to connect to
  client, err := NewClient("127.0.0.1", 3000)
  panicOnError(err)

  key, err := NewKey("test", "demo, "key") // key can be of any supported type
  panicOnError(err)

  // define some bins
  bins := BinMap{
    "bin1": 42, // you can pass any supported type as bin value
    "bin2": "An elephant is a mouse with an operating system",
    "bin3": []interface{}{"Go", 2009},
  }

  // write the bins
  writePolicy := NewWritePolicy(0, 0)
  err = client.Put(writePolicy, key, bins)
  panicOnError(err)

  // read it back!
  readPolicy := NewPolicy()
  rec, err := client.Get(readPolicy, key)
  panicOnError(err)

  // change data
  rec.Bins["bin1"].(int) += 1

  // update
  err = client.Put(nil, key, rec.Bins)
```

<!--
################################################################################
key
################################################################################
-->
<a name="key"></a>

## NewKey(ns, set string, key interface{})

A record is addressable via its key. A key is a struct containing:

- `ns` — The namespace of the key. Must be a String.
- `set` – The set of the key. Must be a String.
- `key` – The value of the key. Can be of any supported types.

Example:

```go
  key, err := NewKey("test", "demo, "key") // key can be of any supported type
  panicOnError(err)
```
