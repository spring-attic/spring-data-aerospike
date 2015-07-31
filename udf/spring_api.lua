-- Copyright 2013-2015 Khosrow Afroozeh.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http:--www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

local function filter_record(rec, filterFuncStr, filterFunc)
  -- if there is no filter, select all records
  if filterFuncStr == nil then
    return true
  end
  -- if there was a filter specified, and was successfully compiled
  if filterFunc ~= nil then
    local context = {rec = rec, selectedRec = false, string = string}

    -- sandbox the function
    setfenv(filterFunc, context)
    filterFunc()

    return context.selectRec
  end

  -- if there was a filter function, but failed to compile
  return true
end

local function parseFieldStatements(fieldValueStatements)
  local fieldFuncs = nil
  if fieldValueStatements ~= nil then
    fieldFuncs = {}
    for fn, exp in map.pairs(fieldValueStatements) do
      fieldFuncs[fn] = load(exp)
    end
  end
  return fieldFuncs
end

function select_records(stream, origArgs)
  local filterFuncStr = origArgs["filterFuncStr"]
  local fieldValueStatements = origArgs["funcStmt"]
  local fields = origArgs["selectFields"]

  local includeAllFields = false
  if origArgs["includeAllFields"] == 1 or origArgs["includeAllFields"] == 'true' then
    includeAllFields = true
  end

  local filterFunc = nil
  if filterFuncStr ~= nil then
    filterFunc = load(filterFuncStr)
  end

  local fieldFuncs = parseFieldStatements(fieldValueStatements)

  local function map_record(rec)

    -- Add name and age to returned map.
    -- Could add other record bins here as well.
    -- This code shows different data access to record bins
    local result = map()
    local addAllFields = false

    if fields ~= nil then
      for v in list.iterator(fields) do
        if fieldFuncs ~= nil and fieldFuncs[v] ~= nil then
          local context = {rec = rec, result = nil}
          local f = fieldFuncs[v]
          -- sandbox the function
          setfenv(f, context)
          f()

          result[v] = context.result
        else
          result[v] = rec[v]
        end
      end
    end

    if (fields == nil) or (includeAllFields == true) then
      local names = record.bin_names(rec)
      for i, v in ipairs(names) do
        result[v] = rec[v]
      end
    end

    return result
  end


  local function filter_records(rec)
    return filter_record(rec, filterFuncStr, filterFunc)
  end

  if filterFuncStr ~= nil then
    return stream : filter(filter_records) : map(map_record)
  else
    return stream : map(map_record)
  end
end

-----------------------------------------------------------------
-----------------------------------------------------------------
function select_agg_records(stream, origArgs)
  local fields = origArgs["selectFields"]
  local groupByFields = origArgs["groupByFields"]
  local aggFieldNames = origArgs["aggregateFields"]

  local includeAllFields = false
  if origArgs["includeAllFields"] == 1 or origArgs["includeAllFields"] == 'true' then
    includeAllFields = true
  end

  local filterFuncStr = origArgs["filterFuncStr"]
  local fieldValueStatements = origArgs["funcStmt"]

  local filterFunc = nil
  if filterFuncStr ~= nil then
    filterFunc = load(filterFuncStr)
  end

  local fieldFuncs = parseFieldStatements(fieldValueStatements)

  local function map_aggregates(rec)

    local accu = map()

    -- there is an optimization here regarding key to avoid
    -- allocating interim keys
    local key = {}
    if groupByFields ~= nil then
      for v in list.iterator(groupByFields) do
        key[#key+1] = '.'
        key[#key+1] = tostring(rec[v])
      end
    end
    key = table.concat(key)

    -- local key = ""
    -- if groupByFields ~= nil then
    --   for v in list.iterator(groupByFields) do
    --     key = key .. '.' .. tostring(rec[v])
    --   end
    -- end

    local info = accu[key]
    if info == nil then
      info = map{key = '', rec = map(), aggs = map({count = 0})}
    end

    if fields ~= nil then
      -- for v in list.iterator(fields) do
      --   info.rec[v] = rec[v]
      -- end
      for v in list.iterator(fields) do
        if fieldFuncs ~= nil and fieldFuncs[v] ~= nil then

          local context = {rec = rec, result = nil}
          local f = fieldFuncs[v]

          -- sandbox the function
          setfenv(f, context)
          f()

          if type(context.result) == "number" then
            info.aggs[v] = (info.aggs[v] or 0) + (context.result or 0)
          else
            info.aggs[v] = (info.aggs[v] or 0)
          end

        else
          info.rec[v] = rec[v]
        end
      end
    end

    if fields == nil or includeAllFields then
      local names = record.bin_names(rec)
      -- for i, v in ipairs(names) do
      for i=1, #names do local v = names[i];
        info.rec[v] = rec[v]
      end
    end

    info.aggs.count = info.aggs.count + 1

    info.key = key
    accu[key] = info

    return accu
  end

  local function accu_tuples(tuple1, tuple2)
    -- accumulate
    for f, v in map.pairs(tuple1.aggs) do
        tuple1.aggs[f] = v + tuple2.aggs[f]
    end

    return tuple1
  end

  local function reduce_aggregates(accu1, accu2)

    for key, tuple in map.pairs(accu2) do

      if accu1[key] ~= nil then
        -- same key, accumulate
        accu1[key] = accu_tuples(accu1[key], tuple)
      else
        accu1[key] = tuple
      end
    end

    return accu1
  end

  local function filter_records(rec)
    return filter_record(rec, filterFuncStr, filterFunc)
  end

  if filterFuncStr ~= nil then
    return stream : filter(filter_records) : map(map_aggregates)  : reduce(reduce_aggregates)
  else
    return stream : map(map_aggregates)  : reduce(reduce_aggregates)
  end
end

------------------------------------------------------------------------------------------
--  Returns Record Digests For Specified Filters
------------------------------------------------------------------------------------------
function query_digests(stream, origArgs)
  local filterFuncStr = origArgs["filterFuncStr"]

  local filterFunc = nil
  if filterFuncStr ~= nil then
    filterFunc = load(filterFuncStr)
  end

  local function add_records(rec)

      local r = map()
      r['d'] =  record.digest(rec)
      
      return r
  end

  local function filter_records(rec)
    return filter_record(rec, filterFuncStr, filterFunc)
  end

  return stream : filter(filter_records) : map(add_records)
end

------------------------------------------------------------------------------------------
--  Returns All bin names
------------------------------------------------------------------------------------------
function query_bin_names(stream, origArgs)
  local filterFuncStr = origArgs["filterFuncStr"]

  local filterFunc = nil
  if filterFuncStr ~= nil then
    filterFunc = load(filterFuncStr)
  end

  local function map_bin_names(bin_names, rec)

      local names = record.bin_names(rec)
      for i=1, #names do 
        bin_names[names[i]] = 0
      end

      return bin_names
  end

  local function reducer(a, b)
    local res = map.merge(a, b)
    return res
  end

  local function filter_records(rec)
    return filter_record(rec, filterFuncStr, filterFunc)
  end

  return stream : filter(filter_records) : aggregate(map {}, map_bin_names) : reduce(reducer)
end

-----------
function update_record(rec, origArgs)
  local fieldValueStatements = origArgs["funcStmt"]
  local fieldFuncs = parseFieldStatements(fieldValueStatements)

  for binName, f in pairs(fieldFuncs) do
    local context = {rec = rec}

    -- sandbox the function
    setfenv(f, context)
    f()
  end

  local status = aerospike:update( rec )
  local results = map {status = status}

  return results
end
