-- read.lua — wrk2 script: POST /query with a SELECT statement

wrk.method = "POST"
wrk.path   = "/query"
wrk.body   = "SELECT * FROM bench_items LIMIT 10"
wrk.headers["Content-Type"] = "application/octet-stream"
