local function putBin(r,name,value)
    if not aerospike:exists(r) then aerospike:create(r) end
    r[name] = value
    aerospike:update(r)
end

-- Set a particular bin
function writeBin(r,name,value)
    putBin(r,name,value)
end

-- Get a particular bin
function readBin(r,name)
    return r[name]
end

-- Return generation count of record
function getGeneration(r)
    return record.gen(r)
end

-- Update record only if gen hasn't changed
function writeIfGenerationNotChanged(r,name,value,gen)
    if record.gen(r) == gen then
        r[name] = value
        aerospike:update(r)
    end
end

-- Set a particular bin only if record does not already exist.
function writeUnique(r,name,value)
    if not aerospike:exists(r) then
        aerospike:create(r)
        r[name] = value
        aerospike:update(r)
    end
end

-- Validate value before writing.
function writeWithValidation(r,name,value)
    if (value >= 1 and value <= 10) then
        putBin(r,name,value)
    else
        error("1000:Invalid value")
    end
end

-- Record contains two integer bins, name1 and name2.
-- For name1 even integers, add value to existing name1 bin.
-- For name1 integers with a multiple of 5, delete name2 bin.
-- For name1 integers with a multiple of 9, delete record.
function processRecord(r,name1,name2,addValue)
    local v = r[name1]

    if (v % 9 == 0) then
        aerospike:remove(r)
        return
    end

    if (v % 5 == 0) then
        r[name2] = nil
        aerospike:update(r)
        return
    end

    if (v % 2 == 0) then
        r[name1] = v + addValue
        aerospike:update(r)
    end
end
