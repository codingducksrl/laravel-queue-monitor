<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use CodingDuck\QueueMonitor\Sinks\Emf\EmfSink;
use CodingDuck\QueueMonitor\Sinks\Emf\Emitter;
use CodingDuck\QueueMonitor\Sinks\Emf\LogEmitter;
use CodingDuck\QueueMonitor\Sinks\Emf\StdoutEmitter;
use CodingDuck\QueueMonitor\Sinks\NullSink;
use Illuminate\Log\LogManager;
use Illuminate\Support\Manager;
use InvalidArgumentException;

class MetricSinkManager extends Manager {
    public function getDefaultDriver(): string {
        $driver = $this->config->get('queue-monitor.sink');

        return is_string($driver) && $driver !== '' ? $driver : 'null';
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

        return new EmfSink(
            $this->createEmitter(),
            is_string($namespace) && $namespace !== '' ? $namespace : 'Laravel/Queue',
            $this->entity(),
        );
    }

    /**
     * @return array<string, string>
     */
    private function entity(): array {
        $configured = $this->config->get('queue-monitor.emf.entity');

        if (! is_array($configured)) {
            return [];
        }

        $entity = [];

        foreach ($configured as $name => $value) {
            if (is_string($name) && is_string($value)) {
                $entity[$name] = $value;
            }
        }

        return $entity;
    }

    protected function createNullDriver(): MetricSink {
        return new NullSink;
    }

    private function createEmitter(): Emitter {
        if ($this->container->bound(Emitter::class)) {
            return $this->container->make(Emitter::class);
        }

        $channel = $this->config->get('queue-monitor.emf.channel');

        if (! is_string($channel) || $channel === '') {
            return new StdoutEmitter;
        }

        return new LogEmitter($this->container->make(LogManager::class), $channel);
    }
}
