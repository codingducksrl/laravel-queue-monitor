<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\Sinks\EmfSink;
use Illuminate\Support\Carbon;

beforeEach(function (): void {
    Carbon::setTestNow(Carbon::parse('2026-09-21T09:00:00Z'));
    $this->out = new SplFileObject('php://memory', 'w+');
    $this->sink = new EmfSink('Acme/Queues', [], $this->out);
});

function queueLevel(): array {
    return ['Connection' => 'redis', 'Queue' => 'default'];
}

function classLevel(string $class): array {
    return ['Connection' => 'redis', 'Queue' => 'default', 'JobClass' => $class];
}

function written(SplFileObject $out): string {
    $out->rewind();

    return (string) $out->fread(1 << 20);
}

function documents(SplFileObject $out): array {
    return array_map(
        fn (string $line): array => json_decode($line, true, 512, JSON_THROW_ON_ERROR),
        array_values(array_filter(explode("\n", written($out)))),
    );
}

it('emits the document shape the EMF specification requires', function (): void {
    $out = new SplFileObject('php://memory', 'w+');

    (new EmfSink('Acme/Queues', ['Service' => 'checkout', 'Environment' => 'production'], $out))->write([
        new Metric('JobsPending', 42, queueLevel()),
        new Metric('JobsCompleted', 165, queueLevel()),
    ]);

    expect(written($out))->toBe(
        '{"Service":"checkout","Environment":"production","Connection":"redis","Queue":"default",'
        .'"JobsPending":42,"JobsCompleted":165,"_aws":{"Timestamp":1789981200000,"CloudWatchMetrics":'
        .'[{"Namespace":"Acme/Queues","Dimensions":[["Connection","Queue"]],"Metrics":'
        .'[{"Name":"JobsPending","Unit":"Count"},{"Name":"JobsCompleted","Unit":"Count"}]}]}}'."\n"
    );
});

it('emits one document per dimension tuple', function (): void {
    $this->sink->write([
        new Metric('JobsCompleted', 165, queueLevel()),
        new Metric('JobsCompleted', 120, classLevel('App\Jobs\SendInvoice')),
        new Metric('JobsCompleted', 45, classLevel('App\Jobs\SyncContact')),
    ]);

    expect(documents($this->out))->toHaveCount(3);
});

it('gives every document exactly one dimension set matching its own tuple', function (): void {
    $this->sink->write([
        new Metric('JobsCompleted', 165, queueLevel()),
        new Metric('JobsCompleted', 120, classLevel('App\Jobs\SendInvoice')),
    ]);

    $sets = array_map(
        static fn (array $document): array => $document['_aws']['CloudWatchMetrics'][0]['Dimensions'],
        documents($this->out),
    );

    expect($sets)->toBe([
        [['Connection', 'Queue']],
        [['Connection', 'Queue', 'JobClass']],
    ]);
});

it('publishes the queue level value exactly once across the batch', function (): void {
    $this->sink->write([
        new Metric('JobsCompleted', 165, queueLevel()),
        new Metric('JobsCompleted', 120, classLevel('App\Jobs\SendInvoice')),
        new Metric('JobsCompleted', 45, classLevel('App\Jobs\SyncContact')),
    ]);

    $published = [];

    foreach (documents($this->out) as $document) {
        foreach ($document['_aws']['CloudWatchMetrics'][0]['Dimensions'] as $set) {
            $tuple = [];

            foreach ($set as $key) {
                $tuple[$key] = $document[$key];
            }

            foreach ($document['_aws']['CloudWatchMetrics'][0]['Metrics'] as $metric) {
                $published[] = $metric['Name'].'|'.json_encode($tuple);
            }
        }
    }

    expect($published)->toHaveCount(count(array_unique($published)))
        ->and(array_count_values($published)['JobsCompleted|'.json_encode(queueLevel())])->toBe(1);
});

it('packs every metric sharing a tuple into one document', function (): void {
    $this->sink->write([
        new Metric('JobsPending', 42, queueLevel()),
        new Metric('JobsDelayed', 7, queueLevel()),
        new Metric('JobsInProgress', 3, queueLevel()),
    ]);

    $documents = documents($this->out);

    expect($documents)->toHaveCount(1)
        ->and($documents[0]['_aws']['CloudWatchMetrics'][0]['Metrics'])->toHaveCount(3)
        ->and($documents[0]['JobsPending'])->toBe(42);
});

it('stamps the current time in epoch milliseconds', function (): void {
    $this->sink->write([new Metric('JobsPending', 1, queueLevel())]);

    expect(documents($this->out)[0]['_aws']['Timestamp'])->toBe(1789981200000);
});

it('writes each document as its own newline terminated line', function (): void {
    $out = new class('php://memory', 'w+') extends SplFileObject {
        public array $writes = [];

        public function fwrite(string $data, mixed $length = null): int|false {
            $this->writes[] = $data;

            return parent::fwrite($data);
        }
    };

    (new EmfSink('Acme/Queues', [], $out))->write([new Metric('JobsPending', 1, queueLevel()), new Metric('JobsPending', 2, classLevel('A'))]);

    expect($out->writes)->toHaveCount(2)->each->toEndWith("}\n");
});

it('groups dimensions that are not valid UTF-8 without failing', function (): void {
    $this->sink->write([new Metric('JobsPending', 1, ['Connection' => 'redis', 'Queue' => "bad\xFF"])]);

    expect(documents($this->out)[0]['Queue'])->toStartWith('bad#');
});

it('sanitises entity values and never lets one replace the metadata', function (): void {
    $out = new SplFileObject('php://memory', 'w+');

    (new EmfSink('Acme/Queues', ['_aws' => 'broken', 'Service' => "check\nout"], $out))
        ->write([new Metric('JobsPending', 1, ['Queue' => 'default'])]);

    $document = documents($out)[0];

    expect($document['_aws'])->toBeArray()->and($document['Service'])->toStartWith('check out#');
});

it('treats a short write as a failure', function (): void {
    $out = new class('php://memory', 'w+') extends SplFileObject {
        public function fwrite(string $data, mixed $length = null): int|false {
            return parent::fwrite(substr($data, 0, 3));
        }
    };

    expect(fn () => (new EmfSink('Acme/Queues', [], $out))->write([new Metric('JobsPending', 1, queueLevel())]))
        ->toThrow(RuntimeException::class, 'Unable to write');
});

it('refuses to write into /dev/null', function (): void {
    expect(fn () => (new EmfSink('Acme/Queues', [], new SplFileObject('/dev/null', 'wb')))->write([new Metric('JobsPending', 1, queueLevel())]))
        ->toThrow(RuntimeException::class, 'appendOutputTo');
})->skipOnWindows();

it('rejects a namespace CloudWatch would drop', function (string $namespace): void {
    expect(fn (): EmfSink => new EmfSink($namespace))->toThrow(InvalidArgumentException::class, 'EMF namespace');
})->with(['', str_repeat('n', 256), "Laravel/Queue\n", 'AWS/SQS', ':Queues', 'Queues!']);

it('strips control characters that would void the whole record', function (): void {
    expect(EmfSink::sanitise("Send\x00Invoice\x1F"))->toStartWith('SendInvoice#')
        ->and(EmfSink::sanitise("a\nb"))->toStartWith('a b#');
});

it('keeps a backslashed job class verbatim', function (): void {
    expect(EmfSink::sanitise('App\Jobs\SendInvoice'))->toBe('App\Jobs\SendInvoice');
});

it('folds an empty dimension value rather than voiding the record', function (string $value): void {
    expect(EmfSink::sanitise($value))->toBe('__unknown__');
})->with(['', '   ', "\t"]);

it('keeps only the printable ASCII CloudWatch accepts', function (): void {
    expect(EmfSink::sanitise('Café'))->toMatch('/^Cafe#[0-9a-f]{8}$/')
        ->and(EmfSink::sanitise("App\\Jobs\\\xFF\xFEBad"))->toStartWith('App\\Jobs\\Bad#')
        ->and(EmfSink::sanitise("\u{202E}evil\u{009B}"))->toStartWith('evil#');
});

it('never collapses two names onto one series', function (): void {
    expect(EmfSink::sanitise('Отчёт'))->not->toBe(EmfSink::sanitise('Счёт'))
        ->and(EmfSink::sanitise('Café'))->not->toBe(EmfSink::sanitise('Cafe'));
});

it('truncates a long value to the dimension limit', function (): void {
    expect(strlen(EmfSink::sanitise(str_repeat('v', 2000))))->toBe(1024)
        ->and(strlen(EmfSink::sanitise(str_repeat('é', 2000))))->toBe(1024);
});
