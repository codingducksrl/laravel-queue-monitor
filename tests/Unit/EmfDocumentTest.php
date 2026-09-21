<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\Sinks\Emf\EmfDocument;
use CodingDuck\QueueMonitor\Unit;

function emfDocument(array $metrics = [], array $dimensions = ['Connection' => 'redis', 'Queue' => 'default']): EmfDocument {
    return new EmfDocument('Acme/Queues', 1789981200000, $dimensions, $metrics ?: [Metric::make('JobsPending', 42, [])]);
}

it('emits the document shape the EMF specification requires', function (): void {
    $document = new EmfDocument(
        'Acme/Queues',
        1789981200000,
        ['Connection' => 'redis', 'Queue' => 'default'],
        [Metric::make('JobsPending', 42, []), Metric::make('JobsCompleted', 165, [])],
        ['Service' => 'checkout', 'Environment' => 'production'],
    );

    expect($document->toJson())->toBe(
        '{"_aws":{"Timestamp":1789981200000,"CloudWatchMetrics":[{"Namespace":"Acme\\/Queues",'
        .'"Dimensions":[["Connection","Queue"]],"Metrics":[{"Name":"JobsPending","Unit":"Count"},'
        .'{"Name":"JobsCompleted","Unit":"Count"}]}]},"Service":"checkout","Environment":"production",'
        .'"Connection":"redis","Queue":"default","JobsPending":42,"JobsCompleted":165}'
    );
});

it('is a single line with nothing around the root object', function (): void {
    $json = emfDocument()->toJson();

    expect($json)->toStartWith('{')->toEndWith('}')->not->toContain("\n");
});

it('declares every dimension key as a string root member', function (): void {
    $decoded = json_decode(emfDocument()->toJson(), true, 512, JSON_THROW_ON_ERROR);

    foreach ($decoded['_aws']['CloudWatchMetrics'][0]['Dimensions'][0] as $key) {
        expect($decoded)->toHaveKey($key)->and($decoded[$key])->toBeString();
    }
});

it('declares every metric name as a numeric root member', function (): void {
    $decoded = json_decode(emfDocument()->toJson(), true, 512, JSON_THROW_ON_ERROR);

    foreach ($decoded['_aws']['CloudWatchMetrics'][0]['Metrics'] as $metric) {
        expect($decoded[$metric['Name']])->toBeNumeric();
    }
});

it('carries the unit through to the directive', function (): void {
    $decoded = json_decode(
        emfDocument([Metric::make('OldestPendingAge', 18, [], Unit::Seconds)])->toJson(),
        true,
        512,
        JSON_THROW_ON_ERROR,
    );

    expect($decoded['_aws']['CloudWatchMetrics'][0]['Metrics'][0]['Unit'])->toBe('Seconds');
});

it('splits a directive that exceeds the metric definition cap', function (): void {
    $metrics = [];

    for ($i = 0; $i < 250; $i++) {
        $metrics[] = Metric::make('Metric'.$i, $i, []);
    }

    $parts = emfDocument($metrics)->chunk();

    expect($parts)->toHaveCount(3);

    $names = [];

    foreach ($parts as $part) {
        expect(count($part->metrics))->toBeLessThanOrEqual(EmfDocument::MAX_METRICS)
            ->and($part->dimensions)->toBe(['Connection' => 'redis', 'Queue' => 'default']);

        foreach ($part->metrics as $metric) {
            $names[] = $metric->name;
        }
    }

    expect($names)->toHaveCount(250)->and(array_unique($names))->toHaveCount(250);
});

it('leaves a directive within the cap alone', function (): void {
    expect(emfDocument()->chunk())->toHaveCount(1);
});

it('rejects more than thirty dimension keys', function (): void {
    $dimensions = [];

    for ($i = 0; $i < 31; $i++) {
        $dimensions['D'.$i] = 'v';
    }

    expect(fn (): EmfDocument => emfDocument([], $dimensions))
        ->toThrow(InvalidArgumentException::class, 'at most 30 keys');
});

it('rejects an empty or oversized namespace', function (string $namespace): void {
    expect(fn (): EmfDocument => new EmfDocument($namespace, 1, ['A' => 'b'], []))
        ->toThrow(InvalidArgumentException::class);
})->with(['', str_repeat('n', 1025)]);

it('rejects an oversized metric name', function (): void {
    expect(fn (): EmfDocument => emfDocument([Metric::make(str_repeat('m', 1025), 1, [])]))
        ->toThrow(InvalidArgumentException::class);
});

it('strips control characters that would void the whole record', function (): void {
    expect(EmfDocument::sanitise("Send\x00Invoice\x1F"))->toBe('SendInvoice')
        ->and(EmfDocument::sanitise("a\nb"))->toBe('ab');
});

it('keeps a backslashed job class verbatim', function (): void {
    expect(EmfDocument::sanitise('App\Jobs\SendInvoice'))->toBe('App\Jobs\SendInvoice');
});

it('folds an empty dimension value rather than voiding the record', function (string $value): void {
    expect(EmfDocument::sanitise($value))->toBe(EmfDocument::UNKNOWN);
})->with(['', '   ', "\t"]);

it('truncates a long value on a character boundary', function (): void {
    $value = EmfDocument::sanitise(str_repeat('é', 2000));

    expect(mb_strlen($value))->toBe(EmfDocument::MAX_DIMENSION_VALUE)
        ->and(mb_check_encoding($value, 'UTF-8'))->toBeTrue();
});
