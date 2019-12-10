#!/usr/bin/env tarantool

local fiber = require('fiber')
local netbox = require('net.box')
local os = require('os')
local queue = require('queue')
local test = require('tap').test()
local tnt = require('t.tnt')


local tube


local function check_result()
    test:plan(2)
    if tube == nil then
        os.exit(-1)
    end

    local ok, res = pcall(tube.drop, tube)
    test:is(ok, true, 'drop empty queue')
    test:is(res, true, 'tube:drop() result is true')

    tnt.finish()
    os.exit(test:check() == true and 0 or -1)
end


local function test_lost_session_id_after_yield(test)
    -- See
    -- https://github.com/tarantool/queue/issues/103
    -- https://github.com/tarantool/tarantool/issues/4627

    -- We must check the results of a test after
    -- the queue._on_consumer_disconnect trigger
    -- has been done.
    -- The type of a triggers queue is LIFO
    box.session.on_disconnect(check_result)

    local listen = 'localhost:1918'
    tnt.cfg{ listen = listen }

    local driver = 'fifottl'
    tube = queue.create_tube('test_tube', driver,
        { if_not_exists = true })

    rawset(_G, 'queue', require('queue'))
    tube:grant('guest', { call = true })

    -- Needed for yielding into
    -- the queue._on_consumer_disconnect trigger
    queue.tube.test_tube:put('1')
    queue.tube.test_tube:put('2')
    local connection = netbox.connect(listen)
    connection:call('queue.tube.test_tube:take')
    connection:call('queue.tube.test_tube:take')

    connection:close()

    fiber.sleep(5)
    -- Fail. Trigger check_result() is a valid exit point
    os.exit(-1)
end


test:test('Lost a session id after yield', test_lost_session_id_after_yield)
