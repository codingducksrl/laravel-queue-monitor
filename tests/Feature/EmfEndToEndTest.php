<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use CodingDuck\QueueMonitor\MetricSink;
use CodingDuck\QueueMonitor\MetricSinkManager;
use CodingDuck\QueueMonitor\Sinks\Emf\Emitter;
use CodingDuck\QueueMonitor\Sinks\Emf\StdoutEmitter;
use Illuminate\Support\Facades\Queue;
use Workbench\App\Jobs\SendInvoice;

it('publishes valid EMF documents for a real queue', function (): void {
    config()->set('queue-monitor.sink', 'emf');
    config()->set('queue-monitor.emf.namespace', 'Acme/Queues');
    config()->set('queue-monitor.emf.entity', ['Service' => 'checkout', 'Environment' => 'testing']);
    config()->set('queue-monitor.queues', ['database' => ['default']]);

    $stream = new SplFileObject('php://memory', 'r+');
    app()->bind(Emitter::class, fn (): Emitter => new StdoutEmitter($stream));
    app()->forgetInstance(MetricSink::class);
    app()->instance(MetricSink::class, app(MetricSinkManager::class)->sink('emf'));

    Queue::connection('database')->push(new SendInvoice);
    app(Counters::class)->increment('database', 'default', MetricName::JobsCompleted, SendInvoice::class);

    $this->artisan('queue-monitor:sample')->assertSuccessful();

    $stream->rewind();
    $lines = array_filter(explode("\n", (string) $stream->fread(65536)));

    expect($lines)->not->toBeEmpty();

    $documents = [];

    foreach ($lines as $line) {
        expect($line)->toStartWith('{')->toEndWith('}');

        $document = json_decode($line, true, 512, JSON_THROW_ON_ERROR);

        assertValidEmf($document);

        $documents[] = $document;
    }

    $queueLevel = $documents[0];

    expect($queueLevel['Service'])->toBe('checkout')
        ->and($queueLevel['Environment'])->toBe('testing')
        ->and($queueLevel['Connection'])->toBe('database')
        ->and($queueLevel['Queue'])->toBe('default')
        ->and($queueLevel['JobsPending'])->toBe(1)
        ->and($queueLevel['JobsCompleted'])->toBe(1);

    $classLevel = array_values(array_filter($documents, fn (array $d): bool => isset($d['JobClass'])));

    expect($classLevel)->toHaveCount(1)
        ->and($classLevel[0]['JobClass'])->toBe(SendInvoice::class)
        ->and($classLevel[0]['JobsCompleted'])->toBe(1);
});

/**
 * The structural rules from the AWS embedded metric format specification.
 */
function assertValidEmf(array $document): void {
    expect($document)->toHaveKey('_aws')
        ->and($document['_aws'])->toHaveKeys(['Timestamp', 'CloudWatchMetrics'])
        ->and($document['_aws']['Timestamp'])->toBeInt()
        ->and($document['_aws']['CloudWatchMetrics'])->not->toBeEmpty();

    foreach ($document['_aws']['CloudWatchMetrics'] as $directive) {
        expect($directive)->toHaveKeys(['Namespace', 'Dimensions', 'Metrics'])
            ->and($directive['Namespace'])->toBeString()->not->toBeEmpty()
            ->and($directive['Dimensions'])->not->toBeEmpty()
            ->and(count($directive['Metrics']))->toBeLessThanOrEqual(100);

        foreach ($directive['Dimensions'] as $set) {
            expect(count($set))->toBeLessThanOrEqual(30);

            foreach ($set as $key) {
                expect($document)->toHaveKey($key)
                    ->and($document[$key])->toBeString()->not->toBeEmpty()
                    ->and(mb_strlen($document[$key]))->toBeLessThanOrEqual(1024);
            }
        }

        foreach ($directive['Metrics'] as $metric) {
            expect($metric)->toHaveKey('Name')
                ->and($document[$metric['Name']] ?? null)->toBeNumeric()
                ->and($metric['Unit'])->toBeIn([
                    'Seconds', 'Microseconds', 'Milliseconds', 'Bytes', 'Kilobytes', 'Megabytes',
                    'Gigabytes', 'Terabytes', 'Bits', 'Kilobits', 'Megabits', 'Gigabits', 'Terabits',
                    'Percent', 'Count', 'Bytes/Second', 'Kilobytes/Second', 'Megabytes/Second',
                    'Gigabytes/Second', 'Terabytes/Second', 'Bits/Second', 'Kilobits/Second',
                    'Megabits/Second', 'Gigabits/Second', 'Terabits/Second', 'Count/Second', 'None',
                ]);
        }
    }
}
