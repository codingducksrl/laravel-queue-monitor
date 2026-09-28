<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\QueueMonitor;
use Illuminate\Config\Repository;
use Illuminate\Container\Container;

function monitor(array $config): QueueMonitor {
    return new QueueMonitor(new Repository(['queue-monitor' => $config, 'queue' => [
        'default' => 'redis',
        'connections' => ['redis' => ['queue' => 'jobs'], 'sqs' => ['queue' => 'orders']],
    ]]), new Container);
}

it('reads the enabled flag the way env() values arrive', function (mixed $value, bool $enabled): void {
    expect(monitor(['enabled' => $value])->enabled())->toBe($enabled);
})->with([
    [true, true], ['1', true], ['true', true], ['on', true], ['yes', true],
    [false, false], ['0', false], ['false', false], [null, false], ['', false],
]);

it('monitors the default queue of the default connection when none are listed', function (): void {
    expect(monitor(['queues' => []])->sampledQueues())->toBe([['redis', 'jobs']]);
});

it('reads connection to queue pairs', function (): void {
    expect(monitor(['queues' => ['redis' => ['default', 'high'], 'sqs' => []]])->sampledQueues())
        ->toBe([['redis', 'default'], ['redis', 'high'], ['sqs', 'orders']]);
});

it('accepts a single queue given as a string', function (): void {
    expect(monitor(['queues' => ['redis' => 'high']])->sampledQueues())->toBe([['redis', 'high']]);
});

it('drops empty and duplicate queue entries', function (): void {
    expect(monitor(['queues' => ['redis' => ['high', '', 'high', 42]]])->sampledQueues())->toBe([['redis', 'high']]);
});

it('rejects a list where a connection map belongs', function (): void {
    expect(fn (): array => monitor(['queues' => ['redis', 'sqs']])->sampledQueues())
        ->toThrow(InvalidArgumentException::class, 'maps connection names');
});
