<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * @method static bool         enabled()
 * @method static list<string> connections()
 * @method static bool         monitors(string $connection)
 *
 * @see \CodingDuck\QueueMonitor\QueueMonitor
 */
class QueueMonitor extends Facade {
    protected static function getFacadeAccessor(): string {
        return \CodingDuck\QueueMonitor\QueueMonitor::class;
    }
}
