<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\QueueMonitor;
use Illuminate\Config\Repository;

it('reads the enabled flag from config', function (): void {
    $monitor = new QueueMonitor(new Repository(['queue-monitor' => ['enabled' => false]]));

    expect($monitor->enabled())->toBeFalse();
});

it('discards non-string connection entries', function (): void {
    $monitor = new QueueMonitor(
        new Repository(['queue-monitor' => ['enabled' => true, 'connections' => ['redis', 42, null]]])
    );

    expect($monitor->connections())->toBe(['redis']);
});

it('monitors every connection when none are listed', function (): void {
    $monitor = new QueueMonitor(new Repository(['queue-monitor' => ['enabled' => true, 'connections' => []]]));

    expect($monitor->monitors('sqs'))->toBeTrue();
});

it('monitors only the listed connections', function (): void {
    $monitor = new QueueMonitor(
        new Repository(['queue-monitor' => ['enabled' => true, 'connections' => ['redis']]])
    );

    expect($monitor->monitors('redis'))->toBeTrue()
        ->and($monitor->monitors('sqs'))->toBeFalse();
});

it('monitors nothing while disabled', function (): void {
    $monitor = new QueueMonitor(
        new Repository(['queue-monitor' => ['enabled' => false, 'connections' => ['redis']]])
    );

    expect($monitor->monitors('redis'))->toBeFalse();
});
