-- Stream aggregations whose last stages run in client-side Lua. Go's client-side
-- Lua has no bytes type, so a vector is a list of its serialized bytes.

local function keep_last(a, b)
  return b
end

-- Returns the value of the given bin, through the client's final reduce.
function stored(stream, bin)
  local function value_of(rec)
    return rec[bin]
  end
  return stream : map(value_of) : reduce(keep_last)
end

-- Returns the argument as client-side Lua sees it.
function argument(stream, value)
  local function id_of(rec)
    return rec['id']
  end
  return stream : map(id_of) : reduce(keep_last) : map(function(_)
    return value
  end)
end
