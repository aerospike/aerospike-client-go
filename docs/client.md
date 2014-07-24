# Client Class

The `Client` class provides operations which can be performed on an Aerospike
database cluster. In order to get an instance of the Client class, you need
to call `NewClient()`:

```go
  client, err := as.Client("127.0.0.1", 3000)
```

With a new client, you can use any of the methods specified below:

- [Methods](#methods)
  - [Add()](#add)
  - [Append()](#append)
  - [Close()](#close)
  - [Delete()](#delete)
  - [Exists()](#exists)
  - [Get()](#get)
  - [IsConnected()](#isConnected)
  - [Prepend()](#prepend)
  - [Put()](#put)
  - [Touch()](#touch)


<a name="methods"></a>
## Methods

<!--
################################################################################
add()
################################################################################
-->
<a name="add"></a>

### Add(policy *WritePolicy, key *Key, bins BinMap) error

Using the provided key, adds values to the mentioned bins.
Bin value types should by of `integer` for the command to have any effect.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A [BinMap](datamodel.md#binmap) used for specifying the fields and value.

Example:
```go
  key := NewKey("test", "demo", 123)

  bins = BinMap {
    "e": 2,
    "pi": 3,
  }

  err := client.Add(nil, key, bins)
```

<!--
################################################################################
append()
################################################################################
-->
<a name="append"></a>

### Append(policy *WritePolicy, key *Key, bins BinMap) error

Using the provided key, appends provided values to the mentioned bins.
Bin value types should by of `string` or `ByteArray` for the command to have any effect.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A [BinMap](datamodel.md#binmap) used for specifying the fields and value.

Example:
```go
  key := NewKey("test", "demo", 123)

  bins = BinMap {
    "story": ", and lived happily ever after...",
  }

  err := client.Append(nil, key, bins)
```

<!--
################################################################################
close()
################################################################################
-->
<a name="close"></a>

### Close()

Closes the client connection to the cluster.

Example:
```go
  client.Close()
```

<!--
################################################################################
remove()
################################################################################
-->
<a name="delete"></a>

### Delete(policy *WritePoicy, key *Key) (existed bool, err error)

Removes a record with the specified key from the database cluster.

Parameters:

- `policy`      – (optional) The [delete Policy object](policies.md#RemovePolicy) to use for this operation.
- `key`         – A [Key object](datamodel.md#key) used for locating the record to be removed.

returned values:

- `existed`         – Boolean value that indicates if the Key existed.

Example:
```go
  key := NewKey("test", "demo", 123)

  if existed, err := client.Delete(nil, key); existed {
    // do something
  }
```

<!--
################################################################################
exists()
################################################################################
-->
<a name="exists"></a>

### Exists(policy *BasePolicy, key *Key) (bool, error)

Using the key provided, checks for the existence of a record in the database cluster .

Parameters:

- `policy`      – (optional) The [BasePolicy object](policies.md#BasePolicy) to use for this operation.
                  Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.

Example:

```go
  key := NewKey("test", "demo", 123)

  if exists, err := client.Exists(nil, key) {
    // do something
  }
```

<!--
################################################################################
get()
################################################################################
-->
<a name="get"></a>

### Get(polict *BasePolicy, key *Key, bins ...string) (*Record, error)

Using the key provided, reads a record from the database cluster .

Parameters:

- `policy`      – (optional) The [BasePolicy object](policies.md#BasePolicy) to use for this operation.
                  Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – (optional) The function to call when the operation completes with the results of the operation.
                Will retrieve all bins if not provided.

Example:

```go
  key := NewKey("test", "demo", 123)

  rec, err := client.Get(nil, key) // reads all the bins
```
<!--
################################################################################
idConnected()
################################################################################
-->
<a name="isConnected"></a>

### IsConnected() bool

Checks if the client is connected to the cluster.

<!--
################################################################################
prepend()
################################################################################
-->
<a name="prepend"></a>

### Prepend(policy *WritePolicy, key *Key, bins BinMap) error

Using the provided key, prepends provided values to the mentioned bins.
Bin value types should by of `string` or `ByteArray` for the command to have any effect.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A [BinMap](datamodel.md#binmap) used for specifying the fields and value.

Example:
```go
  key := NewKey("test", "demo", 123)

  bins = BinMap {
    "story": "Long ago, in a galaxy far far away, ",
  }

  err := client.Prepend(nil, key, bins)
```

<!--
################################################################################
put()
################################################################################
-->
<a name="put"></a>

### Put(policy *WritePolicy, key *Key, bins BinMap) error

Writes a record to the database cluster. If the record exists, it modifies the record with bins provided.
To remove a bin, set its value to `nil`.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A [BinMap map](datamodel.md#binmap) used for specifying the fields to store.

Example:
```go
  key := NewKey("test", "demo", 123)

  bins = BinMap {
    "a": "Lack of skill dictates economy of style.",
    "b": 123,
    "c": []int{1, 2, 3},
    "d": map[string]interface{}{"a": 42, "b": "An elephant is mouse with an operating system."},
  }

  err := client.Put(nil, key, bins)
```

<!--
################################################################################
touch()
################################################################################
-->
<a name="touch"></a>

### Touch(policy *WritePolicy, key *Key) error

Create record if it does not already exist.
If the record exists, the record's time to expiration will be reset to the policy's expiration.

Parameters:

- `policy`      – (optional) A [Write Policy object](policies.md#WritePolicy) to use for this operation.
                Pass `nil` for default values.
- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.

Example:
```go
  key := NewKey("test", "demo", 123)

  err := client.Touch(NewWritePolicy(0, 5), key)
```
