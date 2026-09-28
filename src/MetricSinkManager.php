<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use CodingDuck\QueueMonitor\Sinks\EmfSink;
use Illuminate\Support\Manager;
use InvalidArgumentException;

final class MetricSinkManager extends Manager {
    public function getDefaultDriver(): string {
        $driver = $this->config->get('queue-monitor.sink');

        return is_string($driver) && $driver !== '' ? $driver : 'emf';
    }

    public function sink(?string $name = null): MetricSink {
        $sink = $this->driver($name);

        if (! $sink instanceof MetricSink) {
            throw new InvalidArgumentException(
                sprintf('The [%s] queue-monitor sink must implement [%s].', $name ?? $this->getDefaultDriver(), MetricSink::class)
            );
        }

        return $sink;
    }

    protected function createEmfDriver(): MetricSink {
        $namespace = $this->config->get('queue-monitor.emf.namespace');
        $entity = [];

        foreach ((array) $this->config->get('queue-monitor.emf.entity') as $name => $value) {
            if (is_string($name) && is_string($value)) {
                $entity[$name] = $value;
            }
        }

        return new EmfSink(is_string($namespace) && $namespace !== '' ? $namespace : 'Laravel/Queue', $entity);
    }
}
