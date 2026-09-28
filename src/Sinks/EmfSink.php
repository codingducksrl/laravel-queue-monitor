<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Sinks;

use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\MetricSink;
use Illuminate\Support\Carbon;
use Illuminate\Support\Str;
use InvalidArgumentException;
use RuntimeException;
use SplFileObject;

/**
 * CloudWatch Embedded Metric Format: one JSON line per dimension tuple, which
 * the platform's log driver ships to CloudWatch Logs.
 */
final readonly class EmfSink implements MetricSink {
    private const string UNKNOWN = '__unknown__';

    private const int MAX_DIMENSION_VALUE = 1024;

    /**
     * @param array<string, string> $entity
     */
    public function __construct(
        private string $namespace,
        private array $entity = [],
        private SplFileObject $out = new SplFileObject('php://stdout', 'wb'),
    ) {
        // CloudWatch silently drops every metric in a namespace it does not accept.
        if (! preg_match('#^(?!AWS/)[0-9A-Za-z._\-/\#][0-9A-Za-z._\-/\#: ]{0,254}\z#', $namespace)) {
            throw new InvalidArgumentException(
                "EMF namespace [{$namespace}] must be 1-255 characters of A-Z a-z 0-9 . - _ / # : or space, and must not start with AWS/."
            );
        }
    }

    /**
     * @param list<Metric> $metrics
     */
    public function write(array $metrics): void {
        $timestamp = Carbon::now()->getTimestampMs();
        $lines = [];

        foreach ($this->group($metrics) as [$dimensions, $grouped]) {
            $lines[] = json_encode($this->document($timestamp, $dimensions, $grouped), JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES)."\n";
        }

        // Writing to /dev/null succeeds, so the sampler would drain counters nobody receives.
        if (self::discards($this->out)) {
            throw new RuntimeException(
                'The EMF output stream is /dev/null, so every document would be discarded. The scheduler sends '
                .'a command\'s output there unless told otherwise: add ->appendOutputTo(\'/proc/1/fd/1\') to the schedule.'
            );
        }

        // One write per line: a pipe keeps a write of up to PIPE_BUF bytes from interleaving with other writers.
        foreach ($lines as $line) {
            if ($this->out->fwrite($line) !== strlen($line)) {
                throw new RuntimeException('Unable to write an EMF document to the output stream.');
            }
        }
    }

    /**
     * CloudWatch accepts only printable ASCII in a dimension value and drops
     * the whole record for an empty one, taking every sibling metric with it.
     * A value that had to change keeps a short hash of the original, so two
     * names never collapse onto one series.
     */
    public static function sanitise(string $value): string {
        if (trim($value) === '') {
            return self::UNKNOWN;
        }

        $clean = trim((string) preg_replace('/[^\x20-\x7E]/', '', Str::ascii($value)));

        if ($clean !== $value) {
            $clean = substr($clean, 0, self::MAX_DIMENSION_VALUE - 9).'#'.hash('xxh32', $value);
        }

        return substr($clean, 0, self::MAX_DIMENSION_VALUE);
    }

    /**
     * Every metric in a directive is published against every dimension set in
     * it, so a document carries exactly one. `_aws` goes last so no entity or
     * dimension can replace it.
     *
     * @param array<string, string> $dimensions
     * @param list<Metric>          $metrics
     *
     * @return array<string, mixed>
     */
    private function document(int $timestamp, array $dimensions, array $metrics): array {
        $values = [];

        foreach ($metrics as $metric) {
            $values[$metric->name] = $metric->value;
        }

        return [
            ...array_map(self::sanitise(...), $this->entity),
            ...array_map(self::sanitise(...), $dimensions),
            ...$values,
            '_aws' => [
                'Timestamp' => $timestamp,
                'CloudWatchMetrics' => [[
                    'Namespace' => $this->namespace,
                    'Dimensions' => [array_keys($dimensions)],
                    'Metrics' => array_map(fn (string $name): array => ['Name' => $name, 'Unit' => 'Count'], array_keys($values)),
                ]],
            ],
        ];
    }

    /**
     * @param list<Metric> $metrics
     *
     * @return list<array{0: array<string, string>, 1: list<Metric>}>
     */
    private function group(array $metrics): array {
        /** @var array<string, array{0: array<string, string>, 1: list<Metric>}> $groups */
        $groups = [];

        foreach ($metrics as $metric) {
            $key = serialize($metric->dimensions);

            $groups[$key][0] = $metric->dimensions;
            $groups[$key][1][] = $metric;
        }

        return array_values($groups);
    }

    private static function discards(SplFileObject $out): bool {
        $null = @stat('/dev/null');
        $stat = $out->fstat();

        return $null !== false && $stat['dev'] === $null['dev'] && $stat['ino'] === $null['ino'];
    }
}
