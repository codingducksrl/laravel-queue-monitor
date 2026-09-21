<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * @method static bool                              enabled()
 * @method static list<string>                      connections()
 * @method static bool                              monitors(string $connection)
 * @method static list<array{0: string, 1: string}> sampledQueues()
 * @method static string                            defaultConnection()
 * @method static string                            defaultQueue(string $connection)
 * @method static string                            sink()
 * @method static string|null                       counterStore()
 * @method static string                            counterPrefix()
 * @method static int                               maxJobClasses()
 *
 * @see \CodingDuck\QueueMonitor\QueueMonitor
 */
class QueueMonitor extends Facade {
    protected static function getFacadeAccessor(): string {
        return \CodingDuck\QueueMonitor\QueueMonitor::class;
    }
}
