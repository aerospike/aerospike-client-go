# CDT Operations Guide

This guide provides comprehensive documentation for Complex Data Type (CDT) operations in the Aerospike Go client. CDT operations allow you to efficiently work with Maps and Lists stored in Aerospike bins, performing server-side operations that reduce network traffic and improve performance.

## Table of Contents

1. [Introduction](#introduction)
2. [Map Operations](#map-operations)
   - [Map Creation & Setup](#map-creation--setup)
   - [Map Get Operations](#map-get-operations)
   - [Map Modify Operations](#map-modify-operations)
   - [Map Range Operations](#map-range-operations)
3. [List Operations](#list-operations)
   - [List Creation & Setup](#list-creation--setup)
   - [List Get Operations](#list-get-operations)
   - [List Modify Operations](#list-modify-operations)
   - [List Range Operations](#list-range-operations)
4. [Nested Operations](#nested-operations)
   - [Understanding CDT Context](#understanding-cdt-context)
   - [Nested Map Operations](#nested-map-operations)
   - [Nested List Operations](#nested-list-operations)
5. [Best Practices & Tips](#best-practices--tips)
6. [Quick Reference](#quick-reference)

---

## Introduction

### What are CDT Operations?

CDT (Complex Data Type) operations are server-side operations that allow you to manipulate Maps and Lists stored in Aerospike bins without transferring entire data structures over the network. Instead of reading a map, modifying it in your application, and writing it back, you can perform operations directly on the server.

**Benefits:**
- Reduced network traffic
- Atomic operations
- Better performance
- Simplified code

### Basic Concepts

**Maps**: Key-value pairs, similar to Go's `map[any]any`
- Keys can be strings, integers, or bytes
- Values can be any supported type
- Can be ordered or unordered

**Lists**: Ordered collections, similar to Go's `[]any`
- Indexed by position (0-based)
- Can be ordered or unordered
- Support negative indexing (-1 = last item)

**Nested Structures**: Maps and Lists can contain other Maps and Lists, creating complex nested data structures.

### Using Operate() with CDT Operations

All CDT operations are performed using the `Operate()` method:

```go
import as "github.com/aerospike/aerospike-client-go/v8"

// Perform CDT operations
record, err := client.Operate(policy, key,
    as.MapGetByKeyOp("binName", "key", as.MapReturnType.VALUE),
    // ... more operations
)

if err != nil {
    log.Fatal(err)
}

// Results are in OpResults (a slice)
results := record.Bins["binName"].(as.OpResults)
value := results[0]  // First operation result
```

### Understanding OpResults

**When are OpResults returned?**

`OpResults` is a slice type (`[]interface{}`) that wraps results when you perform **multiple operations on the same bin**. The behavior depends on how many operations target the same bin:

- **Single operation on a bin** → Returns the value directly (string, int, `[]any`, etc.)
- **Multiple operations on the same bin** → Returns `OpResults`, a slice containing each operation's result in order

**Example: Multiple Operations (Returns OpResults)**

```go
record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("profile", "name", as.MapReturnType.VALUE),
    as.MapGetByKeyOp("profile", "age", as.MapReturnType.VALUE),
)

results := record.Bins["profile"].(as.OpResults)  // Wrapped in OpResults
name := results[0].(string)  // First operation
age := results[1].(int)    // Second operation
```

**Example: Single Operation (Returns Value Directly)**

```go
record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("profile", "name", as.MapReturnType.VALUE),
)

name := record.Bins["profile"].(string)  // Direct value, NOT OpResults
```

**Example: Operations on Different Bins (Each Returns Directly)**

```go
record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("profile", "name", as.MapReturnType.VALUE),
    as.MapGetByKeyOp("orders", "count", as.MapReturnType.VALUE),
)

name := record.Bins["profile"].(string)  // Direct value (different bin)
count := record.Bins["orders"].(int)    // Direct value (different bin)
```

**Note:** Even if a single operation returns a slice (like `MapGetByValueOp` with `KEY` return type), it's stored directly as `[]any`, not wrapped in `OpResults`. The `OpResults` wrapper only appears when there are multiple operations on the same bin.

### Policies Overview

**MapPolicy**: Controls how maps are created and written
- `DefaultMapPolicy()` - Unordered map, UPDATE mode
- `NewMapPolicy(order, writeMode)` - Custom order and write mode
- Order types: `UNORDERED`, `KEY_ORDERED`, `KEY_VALUE_ORDERED`
- Write modes: `UPDATE`, `CREATE_ONLY`, `UPDATE_ONLY`

**ListPolicy**: Controls how lists are created and written
- `DefaultListPolicy()` - Unordered list
- `NewListPolicy(order, flags)` - Custom order and flags
- Order types: `ListOrderUnordered`, `ListOrderOrdered`

---

## Map Operations

Maps are key-value data structures ideal for storing structured data like user profiles, product catalogs, configuration settings, and more.

### Map Creation & Setup

#### MapPutOp - Put Single Key-Value Pair

Creates or updates a single key-value pair in a map.

```go
// Put a single key-value pair
_, err := client.Operate(nil, key,
    as.MapPutOp(as.DefaultMapPolicy(), "profile", "name", "John Doe"),
)
```

#### MapPutItemsOp - Put Multiple Key-Value Pairs

Creates or updates multiple key-value pairs in a single operation.

```go
// Put multiple items at once
items := map[any]any{
    "name":  "John Doe",
    "email": "john@example.com",
    "age":   30,
    "city":  "San Francisco",
}

_, err := client.Operate(nil, key,
    as.MapPutItemsOp(as.DefaultMapPolicy(), "profile", items),
)
```

#### MapCreateOp - Create Empty Map

Creates an empty map with a specific policy. Useful when you want to ensure a map exists before operations.

```go
// Create an ordered map
policy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
_, err := client.Operate(nil, key,
    as.MapCreateOp("leaderboard", policy, nil),
)
```

#### Map Policies

**UNORDERED** (default): No specific order, fastest performance
```go
policy := as.DefaultMapPolicy()  // UNORDERED
```

**KEY_ORDERED**: Map is ordered by key values
```go
policy := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE)
```

**KEY_VALUE_ORDERED**: Map is ordered by key, then by value
```go
policy := as.NewMapPolicy(as.MapOrder.KEY_VALUE_ORDERED, as.MapWriteMode.UPDATE)
```

**Use KEY_ORDERED or KEY_VALUE_ORDERED when:**
- You need range queries by key
- You need to maintain sorted order
- You're building leaderboards or rankings

---

### Map Get Operations

#### MapGetByKeyOp - Get Value by Key

Retrieves a value from a map by its key. This is one of the most commonly used map operations.

**Function Signature:**
```go
MapGetByKeyOp(binName string, key interface{}, returnType mapReturnType, ctx ...*CDTContext) *Operation
```

**Parameters:**
- `binName`: Name of the bin containing the map
- `key`: The key to look up (string, int64, or interface{} supported type)
- `returnType`: What to return (see [Return Types](#return-types))
- `ctx`: Optional CDT context for nested maps

**Example 1: Get Value (Most Common)**
```go
// Get a value by key
record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("profile", "email", as.MapReturnType.VALUE),
)

if err != nil {
    log.Fatal(err)
}

email := record.Bins["profile"].(string)
fmt.Println("Email:", email)  // Output: Email: john@example.com
```

**Example 1: E-commerce Product Catalog**
```go
// Store product information
product := map[string]any{
    "name":        "Laptop Pro 15",
    "price":       1299.99,
    "stock":       42,
    "category":    "Electronics",
    "description": "High-performance laptop",
}

key, _ := as.NewKey("store", "products", "prod123")
_, err := client.Operate(nil, key,
    as.MapPutItemsOp(as.DefaultMapPolicy(), "product", product),
)

// Later, retrieve product price
record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("product", "price", as.MapReturnType.VALUE),
)

price := record.Bins["product"].(float64)
fmt.Printf("Price: $%.2f\n", price)
```

**Example 2: Check if Key Exists**
```go
// Check if a key exists
record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("profile", "email", as.MapReturnType.EXISTS),
)

exists := record.Bins["profile"].(bool)

if exists {
    fmt.Println("Email is set")
} else {
    fmt.Println("Email not found")
}
```

**Example 3: Get Key-Value Pair**
```go
// Get both key and value
record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("profile", "name", as.MapReturnType.KEY_VALUE),
)

pairs := record.Bins["profile"].([]as.MapPair)
fmt.Printf("Key: %v, Value: %v\n", pairs[0].Key, pairs[0].Value)
```

**Example 4: Get Multiple Values in One Operation**
```go
// Get multiple values at once
record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("profile", "name", as.MapReturnType.VALUE),
    as.MapGetByKeyOp("profile", "age", as.MapReturnType.VALUE),
    as.MapGetByKeyOp("profile", "email", as.MapReturnType.VALUE),
)

results := record.Bins["profile"].(as.OpResults)
name := results[0].(string)
age := results[1].(int)
email := results[2].(string)

fmt.Printf("Name: %s, Age: %d, Email: %s\n", name, age, email)
```

**Example 5: Working with Integer Keys**
```go
// Map with integer keys (e.g., user IDs)
scores := map[any]any{
    1001: 1500,
    1002: 2300,
    1003: 1800,
}

key, _ := as.NewKey("game", "scores", "leaderboard")
_, err := client.Operate(nil, key,
    as.MapPutItemsOp(as.DefaultMapPolicy(), "scores", scores),
)

// Get score for user 1002
record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("scores", 1002, as.MapReturnType.VALUE),
)

score := record.Bins["scores"].(int)
fmt.Printf("User 1002 score: %d\n", score)

```

#### MapGetByKeyListOp - Get Multiple Values by Key List

Retrieves values for multiple keys in a single operation.

```go
// Get multiple values by key list
keys := []any{"name", "email", "age"}
record, err := client.Operate(nil, key,
    as.MapGetByKeyListOp("profile", keys, as.MapReturnType.VALUE),
)

values := record.Bins["profile"].([]any)
// values[0] = name, values[1] = email, values[2] = age
```

#### MapGetByKeyRangeOp - Get Values by Key Range

Retrieves values for keys within a specified range. Works with ordered maps.

```go
// Get values for keys in range [startKey, endKey)
record, err := client.Operate(nil, key,
    as.MapGetByKeyRangeOp("products", "A", "D", as.MapReturnType.KEY_VALUE),
)

products := record.Bins["products"].([]as.MapPair) // Products with keys A, B, C
```

#### MapGetByIndexOp - Get by Index Position

Retrieves a value by its index position in the map (for ordered maps).

```go
// Get first item (index 0)
record, err := client.Operate(nil, key,
    as.MapGetByIndexOp("leaderboard", 0, as.MapReturnType.KEY_VALUE),
)

topPlayers := record.Bins["leaderboard"].([]as.MapPair)
playerAtIndex := topPlayers[0]
```

#### MapGetByRankOp - Get by Value Rank

Retrieves a value by its rank (sorted by value). Rank 0 is the smallest value, rank -1 is the largest value (highest rank).

```go
// Get item with lowest value (rank 0)
record, err := client.Operate(nil, key,
    as.MapGetByRankOp("scores", 0, as.MapReturnType.KEY_VALUE),
)

// Get item with highest value (rank -1)
record, err := client.Operate(nil, key,
    as.MapGetByRankOp("scores", -1, as.MapReturnType.KEY_VALUE),
)

results := record.Bins["scores"].([]as.MapPair)
topPlayer := results[0]
fmt.Printf("Top player: %v with score %v\n", topPlayer.Key, topPlayer.Value)
```


**Understanding Ranks:**
- Rank 0: Smallest/lowest value
- Rank 1: Second smallest value
- Rank -1: Largest/highest value (most common for leaderboards)
- Rank -2: Second largest value

#### MapGetByValueOp - Get Keys by Value

Finds all keys that have a specific value.

```go
// Find all users with score 100
record, err := client.Operate(nil, key,
    as.MapGetByValueOp("scores", 100, as.MapReturnType.KEY),
)

userIds := record.Bins["scores"].([]any) // All users with score 100
```

#### MapSizeOp - Get Map Size

Returns the number of items in the map.

```go
// Get map size
record, err := client.Operate(nil, key,
    as.MapSizeOp("profile"),
)

size := record.Bins["profile"].(int)
fmt.Printf("Profile has %d fields\n", size)
```

---

### Map Modify Operations

#### MapIncrementOp / MapDecrementOp - Increment/Decrement Values

Atomically increments or decrements a numeric value in a map.

```go
// Increment a counter
_, err := client.Operate(nil, key,
    as.MapIncrementOp(as.DefaultMapPolicy(), "counters", "views", 1),
)

// Decrement inventory
_, err := client.Operate(nil, key,
    as.MapDecrementOp(as.DefaultMapPolicy(), "inventory", "item123", 5),
)
```


#### MapRemoveByKeyOp - Remove by Key

Removes a key-value pair from the map.

```go
// Remove a key
_, err := client.Operate(nil, key,
    as.MapRemoveByKeyOp("profile", "oldField", as.MapReturnType.NONE),
)

// Remove and get the removed value
record, err := client.Operate(nil, key,
    as.MapRemoveByKeyOp("profile", "tempData", as.MapReturnType.VALUE),
)

removedValue := record.Bins["profile"].(string) // Value that was removed
```

#### MapRemoveByValueOp - Remove by Value

Removes all key-value pairs that have a specific value.

```go
// Remove all items with value "inactive"
_, err := client.Operate(nil, key,
    as.MapRemoveByValueOp("status", "inactive", as.MapReturnType.KEY),
)
```

#### MapClearOp - Clear All Items

Removes all items from the map.

```go
// Clear entire map
_, err := client.Operate(nil, key,
    as.MapClearOp("profile"),
)
```

---

### Map Range Operations

#### MapGetByIndexRangeOp - Get by Index Range

Retrieves values within an index range.

```go
// Get first 10 items (indices 0-9)
record, err := client.Operate(nil, key,
    as.MapGetByIndexRangeOp("leaderboard", 0, as.MapReturnType.KEY_VALUE),
)

results := record.Bins["leaderboard"].([]as.MapPair)
```

#### MapGetByRankRangeOp - Get by Rank Range

Retrieves values within a rank range (sorted by value). Use rank -1 to start from the highest value.

```go
// Get top 5 players (ranks 0-4, lowest to highest)
record, err := client.Operate(nil, key,
    as.MapGetByRankRangeOp("scores", 0, as.MapReturnType.KEY_VALUE),
)

// Get bottom 5 players (ranks -5 to -1, highest to lowest)
record, err = client.Operate(nil, key,
    as.MapGetByRankRangeOp("scores", -5, as.MapReturnType.KEY_VALUE),
)
```

**Alternative: Using MapGetByRankRangeCountOp for Top N**
```go
// Get top 10 players using count (more intuitive)
// Start from rank -1 (highest) and get 10 items going backwards
record, err := client.Operate(nil, key,
    as.MapGetByRankRangeCountOp("leaderboard", -1, 10, as.MapReturnType.KEY_VALUE),
)

results := record.Bins["leaderboard"].(as.OpResults)
topPlayers := results[0].([]as.MapPair)

// Results are ordered from highest to lowest
for i, player := range topPlayers {
    fmt.Printf("%d. %v: %v\n", i+1, player.Key, player.Value)
}
```

#### MapGetByValueRangeOp - Get by Value Range

Retrieves items with values within a specified range.

```go
// Get all products with price between 10 and 50
record, err := client.Operate(nil, key,
    as.MapGetByValueRangeOp("products", 10.0, 50.0, as.MapReturnType.KEY_VALUE),
)
```

#### MapGetByKeyRelativeIndexRangeOp - Relative Index Range

Retrieves items relative to a key's position.

```go
// Get 3 items starting from key "user123"
record, err := client.Operate(nil, key,
    as.MapGetByKeyRelativeIndexRangeOp("users", "user123", 0, as.MapReturnType.KEY_VALUE),
)
```

---

## List Operations

Lists are ordered collections ideal for queues, timelines, ordered data, and sequences.

### List Creation & Setup

#### ListAppendOp - Append Items

Adds items to the end of a list.

```go
// Append single item
_, err := client.Operate(nil, key,
    as.ListAppendOp("tasks", "task1"),
)

// Append multiple items
_, err = client.Operate(nil, key,
    as.ListAppendOp("tasks", "task2", "task3", "task4"),
)
```

**Real-world example: Task Queue**
```go
taskKey, _ := as.NewKey("app", "tasks", "queue1")
_, err := client.Operate(nil, taskKey,
    as.ListAppendOp("queue", "process_order", "send_email", "update_inventory"),
)
```

#### ListInsertOp - Insert at Index

Inserts items at a specific index position.

```go
// Insert at beginning (index 0)
_, err := client.Operate(nil, key,
    as.ListInsertOp("tasks", 0, "high_priority_task"),
)

// Insert at specific position
_, err = client.Operate(nil, key,
    as.ListInsertOp("tasks", 2, "new_task"),
)
```

#### ListCreateOp - Create Empty List

Creates an empty list with a specific order.

```go
// Create ordered list
_, err := client.Operate(nil, key,
    as.ListCreateOp("queue", as.ListOrderOrdered, false),
)
```

#### List Policies

**UNORDERED** (default): No specific order
```go
policy := as.DefaultListPolicy()  // UNORDERED
```

**ORDERED**: List maintains order
```go
policy := as.NewListPolicy(as.ListOrderOrdered, 0)
```

---

### List Get Operations

#### ListGetOp - Get by Index

Retrieves a value by its index position. Supports negative indexing (-1 = last item).

```go
// Get first item (index 0)
record, err := client.Operate(nil, key,
    as.ListGetOp("tasks", 0),
)

firstTask := record.Bins["tasks"].(string)
fmt.Println("firstTask: ", firstTask)

// Get last item (index -1)
record, err = client.Operate(nil, key,
    as.ListGetOp("tasks", -1),
)
```

#### ListGetRangeOp - Get Range of Items

Retrieves a range of items from the list.

```go
// Get first 10 items (from index 0, count 10)
record, err := client.Operate(nil, key,
    as.ListGetRangeOp("tasks", 0, 10),
)

results := record.Bins["tasks"].([]any)
firstTask := results[0].(string)
```

#### ListGetByIndexOp - Get by Index with Return Type

Similar to `ListGetOp` but with explicit return type control.

```go
// Get index of a value
record, err := client.Operate(nil, key,
    as.ListGetByValueOp("single", "high_priority_task", as.ListReturnTypeIndex)
)
```

#### ListGetByRankOp - Get by Rank

Retrieves an item by its rank (sorted by value).

```go
// Get item with lowest value (rank 0)
record, err := client.Operate(nil, key,
    as.ListGetByRankOp("scores", 0, as.ListReturnTypeValue),
)
```

#### ListGetByValueOp - Get by Value

Finds the index of items with a specific value.

```go
// Find index of value "target"
record, err := client.Operate(nil, key,
    as.ListGetByValueOp("items", "target", as.ListReturnTypeIndex),
)

indices := record.Bins["items"].([]any) // All indices where value appears
```

#### ListSizeOp - Get List Size

Returns the number of items in the list.

```go
// Get list size
record, err := client.Operate(nil, key,
    as.ListSizeOp("tasks"),
)

size := record.Bins["queue"].(int)
fmt.Printf("Queue has %d tasks\n", size)
```

---

### List Modify Operations

#### ListSetOp - Set Value at Index

Updates the value at a specific index.

```go
// Update item at index 0
_, err := client.Operate(nil, key,
    as.ListSetOp("tasks", 0, "updated_task"),
)
```

#### ListRemoveOp - Remove by Index

Removes an item at a specific index.

```go
// Remove first item
_, err := client.Operate(nil, key,
    as.ListRemoveOp("tasks", 0),
)

// Remove last item
_, err = client.Operate(nil, key,
    as.ListRemoveOp("tasks", -1),
)
```

#### ListRemoveByValueOp - Remove by Value

Removes all items with a specific value.

```go
// Remove all occurrences of "completed"
_, err := client.Operate(nil, key,
    as.ListRemoveByValueOp("tasks", "completed", as.ListReturnTypeCount),
)
```

#### ListPopOp - Pop Item

Removes and returns an item (typically used for queues).

```go
// Pop from front (index 0)
record, err := client.Operate(nil, key,
    as.ListPopOp("queue", 0),
)

item := record.Bins["queue"].(string) // Popped item
```

#### ListIncrementOp - Increment Numeric Value

Increments a numeric value at a specific index.

```go
// Increment counter at index 0
_, err := client.Operate(nil, key,
    as.ListIncrementOp("counters", 0, 1),
)
```

#### ListClearOp - Clear All Items

Removes all items from the list.

```go
// Clear entire list
_, err := client.Operate(nil, key,
    as.ListClearOp("tasks"),
)
```

---

### List Range Operations

#### ListGetByIndexRangeOp - Get by Index Range

Retrieves items within an index range.

```go
// Get items from index 5 to 15
record, err := client.Operate(nil, key,
    as.ListGetByIndexRangeOp("items", 5, as.ListReturnTypeValue),
)
```

#### ListGetByRankRangeOp - Get by Rank Range

Retrieves items within a rank range (sorted by value).

```go
// Get top 5 items (ranks 0-4)
record, err := client.Operate(nil, key,
    as.ListGetByRankRangeOp("scores", 0, as.ListReturnTypeValue),
)
```

#### ListGetByValueRangeOp - Get by Value Range

Retrieves items with values within a specified range.

```go
// Get items with values between 10 and 50
record, err := client.Operate(nil, key,
    as.ListGetByValueRangeOp("prices", 10.0, 50.0, as.ListReturnTypeValue),
)
```

---

## Nested Operations

Nested operations allow you to work with Maps and Lists that contain other Maps and Lists, creating complex data structures.

### Understanding CDT Context

CDT Context specifies the path to a nested Map or List within a data structure. You chain contexts to navigate deep into nested structures.

**Context Types:**
- `CtxMapKey(key)` - Navigate to a map value by key
- `CtxListIndex(index)` - Navigate to a list item by index
- `CtxMapRank(rank)` - Navigate to a map value by rank
- `CtxListRank(rank)` - Navigate to a list item by rank

**Example Structure:**
```
bin = {
  "user123": {
    "profile": {
      "name": "John",
      "address": {
        "city": "SF",
        "zip": "94102"
      }
    },
    "orders": [
      {"id": "ord1", "total": 100},
      {"id": "ord2", "total": 200}
    ]
  }
}
```

To access `zip` in the nested address map:
```go
ctx := []*as.CDTContext{
    as.CtxMapKey(as.StringValue("user123")),
    as.CtxMapKey(as.StringValue("profile")),
    as.CtxMapKey(as.StringValue("address")),
}

record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("data", "zip", as.MapReturnType.VALUE, ctx...),
)
```

### Nested Map Operations

#### Example: User with Nested Address Map

```go
// Store nested structure
userData := map[any]any{
    "name": "John Doe",
    "address": map[string]any{
        "street": "123 Main St",
        "city":   "San Francisco",
        "zip":    "94102",
    },
}

key, _ := as.NewKey("app", "users", "user123")
_, err := client.Operate(nil, key,
    as.MapPutItemsOp(as.DefaultMapPolicy(), "user", userData),
)

// Get nested city
ctx := []*as.CDTContext{
    as.CtxMapKey(as.StringValue("address")),
}

record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("user", "city", as.MapReturnType.VALUE, ctx...),
)

results := record.Bins["user"].(map[any]any)
city := results["city"].(string)
fmt.Println("City:", city)  // Output: City: San Francisco
```

#### Example: Multi-Level Configuration

```go
// Store nested configuration
config := map[any]any{
    "database": map[string]any{
        "primary": map[string]any{
            "host": "db1.example.com",
            "port": 3306,
        },
        "replica": map[string]any{
            "host": "db2.example.com",
            "port": 3306,
        },
    },
}

key, _ := as.NewKey("app", "config", "production")
_, err := client.Operate(nil, key,
    as.MapPutItemsOp(as.DefaultMapPolicy(), "config", config),
)

// Get primary database host
ctx := []*as.CDTContext{
    as.CtxMapKey(as.StringValue("database")),
    as.CtxMapKey(as.StringValue("primary")),
}

record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("config", "host", as.MapReturnType.VALUE, ctx...),
)

host := record.Bins["config"].(string)
fmt.Println("Primary DB host:", host)
```

### Nested List Operations

#### Example: Matrix/2D Array Operations

```go
// Store 2D array (list of lists)
matrix := [][]int{
    []int{1, 2, 3},
    []int{4, 5, 6},
    []int{7, 8, 9},
}

key, _ := as.NewKey("app", "data", "matrix1")
_ = client.Put(nil, key, as.BinMap{"matrix": matrix})

// Append to second row (index 1)
ctx := []*as.CDTContext{
    as.CtxListIndex(1),
}

_, err = client.Operate(nil, key,
    as.ListAppendOp("matrix", 10),
)

// Get value from row 1, column 2
ctx = []*as.CDTContext{
    as.CtxListIndex(1),
}

record, err := client.Operate(nil, key,
    as.ListGetOp("matrix", 2, ctx...),
)

value := record.Bins["matrix"]
fmt.Println("Value at [1][2]:", value)  // Output: Value at [1][2]: 6
```

#### Example: User with Nested Order History

```go
// Store user with order history (list of order maps)
user := map[string]any{
    "name": "John Doe",
    "orders": []any{
        map[string]any{"id": "ord1", "total": 100},
        map[string]any{"id": "ord2", "total": 200},
    },
}

key, _ := as.NewKey("store", "users", "user123")
_, err := client.Put(nil, key, as.BinMap{"user": user})

// Add new order to history
newOrder := map[string]any{
    "id":    "ord3",
    "total": 150,
}

ctx := []*as.CDTContext{
    as.CtxMapKey(as.StringValue("orders")),
}

lp := as.NewListPolicy(
    as.ListOrderUnordered,
    as.ListWriteFlagsAddUnique,
)

_, err := client.Operate(nil, key,
    as.ListAppendWithPolicyContextOp(lp, "user", ctx, newOrder),
)

record, _ := client.Operate(nil, key,
		as.ListGetByIndexRangeOp(
			"user",
			0,
			as.ListReturnTypeValue,
			ctx...,
		),
	)

orders := record.Bins["user"].([]any)
lastOrder := orders[2].(map[string]any)
fmt.Println(lastOrder["id"]) //ord3
fmt.Println(orders) // [map[id:ord1 total:100] map[id:ord2 total:200] map[id:ord3 total:150]]

```

#### Example: Getting Nested Values Directly with Chained Context

When you have a map containing a list of maps, you can use chained context to get a specific value directly without retrieving the entire nested structure.

```go
// Store order: map containing list of items (each item is a map)
order := map[string]any{
    "orderId": "ord123",
    "customerId": "cust456",
    "items": []any{
        map[string]any{"productId": "prod1", "quantity": 2, "price": 10.0},
        map[string]any{"productId": "prod2", "quantity": 1, "price": 20.0},
    },
    "total": 40.0,
}

key, _ := as.NewKey("store", "orders", "ord123")
_, err := client.Put(nil, key, as.BinMap{"order": order})

// Get productId from first item directly using chained context
// Context chain: items (map key) -> [0] (list index) -> productId (map key)
ctx := []*as.CDTContext{
    as.CtxMapKey(as.StringValue("items")),  // Step 1: Navigate to "items" list
    as.CtxListIndex(0),                     // Step 2: Navigate to first item (index 0)
}

record, err := client.Operate(nil, key,
    as.MapGetByKeyOp("order", "productId", as.MapReturnType.VALUE, ctx...),
)

productId := record.Bins["order"].(string)
fmt.Println("First product ID:", productId)

// Get quantity from second item
ctx = []*as.CDTContext{
    as.CtxMapKey(as.StringValue("items")),
    as.CtxListIndex(1),  // Second item (index 1)
}

record, err = client.Operate(nil, key,
    as.MapGetByKeyOp("order", "quantity", as.MapReturnType.VALUE, ctx...),
)

quantity := record.Bins["order"].(int)
fmt.Println("Second item quantity:", quantity)
```
---

## Best Practices & Tips

### Performance Considerations

1. **Use server-side operations**: Prefer CDT operations over read-modify-write patterns
2. **Batch operations**: Combine multiple operations in a single `Operate()` call
3. **Choose appropriate policies**: Use UNORDERED when order doesn't matter for better performance
4. **Limit nested depth**: Deep nesting can impact performance

### Type Assertions for Results

Results from CDT operations may need type assertions:

```go
results := record.Bins["profile"].(as.OpResults)

// String values
name := results[0].(string)

// Integer values (server returns as int)
age := results[0].(int)

// Float values
price := results[0].(float64)

// Boolean values
active := results[0].(bool)

// Map values
profile := results[0].(map[any]any)

// List values
items := results[0].([]any)

// MapPair for KEY_VALUE return type
pair := results[0].(as.MapPair)
```
---

## Quick Reference

### Operation Summary

#### Map Operations

| Operation | Description | Use Case |
|-----------|-------------|----------|
| `MapPutOp` | Put single key-value | Update one field |
| `MapPutItemsOp` | Put multiple key-values | Initialize or bulk update |
| `MapGetByKeyOp` | Get value by key | **Most common - retrieve value** |
| `MapGetByKeyListOp` | Get multiple values | Batch retrieval |
| `MapGetByKeyRangeOp` | Get by key range | Range queries |
| `MapIncrementOp` | Increment value | Counters, scores |
| `MapDecrementOp` | Decrement value | Inventory, counters |
| `MapRemoveByKeyOp` | Remove by key | Delete field |
| `MapSizeOp` | Get map size | Count items |

#### List Operations

| Operation | Description | Use Case |
|-----------|-------------|----------|
| `ListAppendOp` | Append items | Add to end |
| `ListInsertOp` | Insert at index | Add at position |
| `ListGetOp` | Get by index | Retrieve item |
| `ListGetRangeOp` | Get range | Get multiple items |
| `ListPopOp` | Pop item | Queue processing |
| `ListRemoveOp` | Remove by index | Delete item |
| `ListSizeOp` | Get list size | Count items |

### Return Type Quick Reference

#### Map Return Types

- `VALUE` - The actual value (most common)
- `KEY` - The key itself
- `KEY_VALUE` - Both key and value as MapPair
- `INDEX` - Index position (ordered maps)
- `RANK` - Rank by value
- `COUNT` - Number of items (1 or 0)
- `EXISTS` - Boolean existence check

#### List Return Types

- `VALUE` - The actual value
- `INDEX` - Index position(s)
- `RANK` - Rank by value
- `COUNT` - Number of items
- `EXISTS` - Boolean existence check

### Context Helper Functions

The following are commonly used context helper functions for navigating nested CDT structures. There are additional helper functions available beyond these basic ones:

- `CtxMapKey(key)` - Navigate to map by key
- `CtxListIndex(index)` - Navigate to list by index
- `CtxMapRank(rank)` - Navigate to map by rank
- `CtxListRank(rank)` - Navigate to list by rank

For a complete list of all available context helper functions, refer to the [API documentation](https://pkg.go.dev/github.com/aerospike/aerospike-client-go/v8#CDTContext).

---

## Conclusion

CDT operations provide powerful, efficient ways to work with Maps and Lists in Aerospike. By performing operations server-side, you reduce network traffic and improve performance. 

**Key Takeaways:**
- Use `MapGetByKeyOp` for the most common map retrieval operations
- Leverage nested operations with CDT Context for complex data structures
- Choose appropriate return types to get exactly the data you need
- Batch operations in a single `Operate()` call for better performance
- Always handle errors and check for nil results
