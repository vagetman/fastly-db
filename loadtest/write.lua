-- write.lua — wrk script: POST /query with INSERT statements
-- Each request inserts a row with a unique id derived from the thread/request counter.

local counter = 0

wrk.method = "POST"
wrk.path   = "/query"
wrk.headers["Content-Type"] = "application/octet-stream"

function request()
    counter = counter + 1
    local id = math.random(100000, 999999)
    local body = string.format(
        "INSERT INTO bench_items VALUES (%d, 'load-%d', %0.2f)",
        id, counter, counter * 0.77
    )
    return wrk.format("POST", "/query", wrk.headers, body)
end
