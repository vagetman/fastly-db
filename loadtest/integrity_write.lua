-- integrity_write.lua — wrk script for UUIDv7 + snapshot integrity testing
--
-- Each request INSERTs a row with a random unique ID.  After the run,
-- the orchestrator (integrity.sh) verifies every successful write survived
-- compaction and cross-PoP replication.
--
-- Environment:
--   INTEGRITY_LOC  — location tag embedded in each row (default: "loc0")
--
-- Usage:
--   INTEGRITY_LOC=nyc wrk -t4 -c20 -d300s \
--       -s loadtest/integrity_write.lua https://your-service.edgecompute.app

local loc = os.getenv("INTEGRITY_LOC") or "loc0"
local run = os.getenv("INTEGRITY_RUN") or ""
local hex_chars = "0123456789abcdef"

-- wrk execution model note:
-- - `init()` and `response()` run in per-thread Lua states.
-- - `done()` runs in a separate "main" Lua state.
-- Therefore counters must be per-thread globals and aggregated via thread:get().
local threads = {}
thr = ""

function setup(thread)
    table.insert(threads, thread)
end

wrk.method  = "POST"
wrk.path    = "/query"
wrk.headers["Content-Type"] = "application/octet-stream"

function init(args)
    counter = 0
    ok_count = 0
    err_count = 0
    -- Track which seq numbers were acknowledged with 200.
    -- With strict mode (one connection per thread), requests are serial:
    -- request() → send → response() → request() → ...
    -- So `pending_seq` always holds the seq of the in-flight request.
    pending_seq = 0
    first_ok_seq = 0
    last_ok_seq = 0

    -- Seed RNG uniquely per thread: table address differs across OS threads
    local addr = tonumber(tostring({}):match("0x(%x+)"), 16) or 0
    thr = string.format("%x", addr)
    math.randomseed(os.time() * 1000 + addr)
    for i = 1, 20 do math.random() end   -- warm up generator
end

-- 16 hex chars = 64 bits of entropy; birthday-collision safe up to ~4 billion rows
local function rand_hex(n)
    local t = {}
    for i = 1, n do
        local idx = math.random(1, 16)
        t[i] = hex_chars:sub(idx, idx)
    end
    return table.concat(t)
end

function request()
    counter = counter + 1
    pending_seq = counter
    local id = loc .. "_" .. rand_hex(16)
    -- Escape single quotes in loc to prevent SQL injection
    local safe_loc = loc:gsub("'", "''")
    local safe_run = run:gsub("'", "''")
    local safe_thr = thr:gsub("'", "''")
    local body = string.format(
        "INSERT INTO integrity_test VALUES ('%s', '%s', '%s', '%s', %d)",
        safe_run, id, safe_loc, safe_thr, counter
    )
    return wrk.format("POST", "/query", wrk.headers, body)
end

function response(status, headers, body)
    if status == 200 then
        ok_count = ok_count + 1
        if first_ok_seq == 0 then
            first_ok_seq = pending_seq
        end
        last_ok_seq = pending_seq
    else
        err_count = err_count + 1
        -- Log first 10 errors for debugging
        if err_count <= 10 then
            io.stderr:write(string.format(
                "[integrity] HTTP %d: %s\n", status, body:sub(1, 200)
            ))
        end
    end
end

function done(summary, latency, requests)
    local ok_total = 0
    local err_total = 0
    local attempted_total = 0
    for _, thread in ipairs(threads) do
        attempted_total = attempted_total + (thread:get("counter") or 0)
        ok_total = ok_total + (thread:get("ok_count") or 0)
        err_total = err_total + (thread:get("err_count") or 0)
    end

    local inflight_total = attempted_total - ok_total - err_total

    io.stderr:write(string.format(
        "[integrity] loc=%s  ok=%d  errors=%d  total=%d  inflight=%d\n",
        loc, ok_total, err_total, ok_total + err_total, inflight_total
    ))

    -- Machine-readable aggregated count file for the orchestrator.
    local path
    if run ~= "" then
        path = string.format("/tmp/integrity_%s_%s.count", run, loc)
    else
        path = string.format("/tmp/integrity_%s.count", loc)
    end
    local f = io.open(path, "w")
    if f then
        f:write(tostring(ok_total) .. "\n")
        f:close()
    end

    -- Machine-readable meta file for strict verification.
    local meta_path
    if run ~= "" then
        meta_path = string.format("/tmp/integrity_%s_%s.meta", run, loc)
    else
        meta_path = string.format("/tmp/integrity_%s.meta", loc)
    end
    local mf = io.open(meta_path, "w")
    if mf then
        mf:write(string.format("attempted=%d\n", attempted_total))
        mf:write(string.format("ok=%d\n", ok_total))
        mf:write(string.format("err=%d\n", err_total))
        mf:write(string.format("inflight=%d\n", inflight_total))

        -- Per-thread stats (used for strict validation).
        for _, thread in ipairs(threads) do
            local t = thread:get("thr") or ""
            local a = thread:get("counter") or 0
            local o = thread:get("ok_count") or 0
            local e = thread:get("err_count") or 0
            local inf = a - o - e
            local fos = thread:get("first_ok_seq") or 0
            local los = thread:get("last_ok_seq") or 0
            if t ~= "" then
                mf:write(string.format("thr=%s attempted=%d ok=%d err=%d inflight=%d first_ok_seq=%d last_ok_seq=%d\n", t, a, o, e, inf, fos, los))
            end
        end
        mf:close()
    end
end
