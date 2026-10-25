-- wrk script: send `depth` pipelined requests per write (TechEmpower "plaintext" style).
--
--   wrk -t1 -c64 -d10s -s bench/pipeline.lua http://127.0.0.1:9000/bench -- 16
init = function(args)
  local depth = tonumber(args[1]) or 1
  local r = {}
  for i = 1, depth do r[i] = wrk.format(nil) end
  req = table.concat(r)
end

request = function() return req end
