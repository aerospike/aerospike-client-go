# Change history

## Dec 10 2014

  * **Fixes**:

    * Fixed issue when the size of key field would not be estimated correctly when WritePolicy.SendKey was set.

## Nov 27 2014

  Major Performance Enhancements. Minor new features and fixes.

  * **Improvements**

    * Go client is much faster and more memory efficient now.
      In some workloads, it competes and wins against C and Java clients.

    * Complex objects are now de/serialized much faster.

  * **New Features**

    * Added Default Policies for Client object.
      Instead of creating a new policy when the passed policy is nil, default policies will be used.

## Nov 24 2014

  * **Fixes**:

    * Fixed issue when WritePolicy.SendKey = true was not respected in Touch() and Operate()

## Nov 22 2014

  Hotfix in unpacker. Update strongly recommended for everyone using Complex objects, LDTs and UDFs.

  * **Fixes**:

    * When Blob, ByteArray or String size has a bit sign set, unpacker reads it wrong.
        Note: This bug only affects unpacking of these objects. Packing was unaffected, and data in the database is valid.

## Nov 2 2014

  Minor, but very impoortant fix.

  * **Fixes**:

    * Node selection in partition map was flawed on first refresh.

  * **Incompatible changes**:

    * `Expiration` and `Generation` in `WritePolicy` are now `int32`
    * `TaskId` in `Statement` is now always set in the client, and is `int64`

  * **New Features**:

    * float32, float64 and bool are now supported in map and array types

## Oct 15 2014 (Beta 2)

  * **Hot fix**:

    * Fixed pack/unpack for uint64

## Aug 20 2014 (Beta 1)

  Major changes and improvements.

  * **New Features**:

    * Added client.Query()
    * Added client.ScanNode()/All()
    * Added client.Operate()
    * Added client.CreateIndex()
    * Added client.DropIndex()
    * Added client.RegisterUDF()
    * Added client.RegisterUDFFromFile()
    * Added client.Execute()
    * Added client.ExecuteUDF()
    * Added client.BatchGet()
    * Added client.BatchGetHeader()
    * Added client.BatchExists()
    * Added LDT implementation
    * Added `Node` and `Key` references to the Record

  * **Changes**:

    * Many minor and major bug fixes
    * Potentially breaking change: Reduced Undocumented API surface
    * Fixed a few places where error results were not checked
    * Breaking Change: Convert Key.namespace & Key.setName from pointer to string; affects Key API
    * Renamed all `this` receivers to appropriate names
    * Major performance improvements (~2X improvements in speed and memory consumption):
      * better memory management for commands; won't allocate if capacity is big enough
      * better hash management in key; avoids two redundant memory allocs
      * use a buffer pool to reduce GC load
      * fine-grained, customizable and deterministic buffer pool implementation for command

    * Optimizations for Key & Digest
      * changed digest implementation, removed an allocation
      * Added RIPEMD160 hash files from crypto to lib
      * pool hash objects

    * Various Benchmark tool improvements
      * now profileable using localhost:6060
      * minor bug fixes

## Jul 26 2014 (Alpha)

  * Initial Release.
