#!/usr/bin/env tarantool

local fiber = require('fiber')
local netbox = require('net.box')
local os = require('os')
local queue = require('queue')
local test = require('tap').test()
local tnt = require('t.tnt')


test:plan(1)

local listen = 'localhost:1918'
tnt.cfg{ listen = listen }


local function test_take_task_after_disconnect(test)
    test:plan(1)
    local driver = 'fifottl'
    local tube = queue.create_tube('test_tube', driver,
        { if_not_exists = true })
    rawset(_G, 'queue', require('queue'))
    tube:grant('guest', { call = true })
    queue.tube.test_tube:put('test_data')

    local connection = netbox.connect(listen)
    local fiber_1 = fiber.create(function()
            connection:call('queue.tube.test_tube:take')
            connection:call('queue.tube.test_tube:take')
        end)

    fiber.sleep(0.1)
    connection:close()
    fiber.set_joinable(fiber_1, true)
    fiber.kill(fiber_1)
    fiber.join(fiber_1)
    fiber.sleep(0.1)

    test:is((box.space.test_tube:select()[1][2]) == 'r', true, 'Task in ready state')
end


test:test('Don\'t take a task after disconnect', test_take_task_after_disconnect)


tnt.finish()
os.exit(test:check() == true and 0 or -1)
