<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

/**
 * Backed by CloudWatch's unit strings, which are a closed set.
 */
enum Unit: string {
    case Count = 'Count';
    case Seconds = 'Seconds';
    case Milliseconds = 'Milliseconds';
    case Percent = 'Percent';
    case None = 'None';
}
