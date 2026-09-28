<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use CodingDuck\QueueMonitor\Sinks\Emf\EmfDocument;
use CodingDuck\QueueMonitor\Sinks\Emf\EmfSink;
use CodingDuck\QueueMonitor\Sinks\Emf\Emitter;
use CodingDuck\QueueMonitor\Sinks\Emf\LogEmitter;
use CodingDuck\QueueMonitor\Sinks\Emf\StdoutEmitter;
use CodingDuck\QueueMonitor\Sinks\NullSink;
use DateTimeImmutable;
use Illuminate\Log\Logger;
use Illuminate\Log\LogManager;
use Illuminate\Support\Manager;
use InvalidArgumentException;
use Monolog\Handler\BufferHandler;
use Monolog\Handler\FingersCrossedHandler;
use Monolog\Handler\NullHandler;
use Monolog\Handler\WhatFailureGroupHandler;
use Monolog\Level;
use Monolog\Logger as MonologLogger;
use Monolog\LogRecord;
use Psr\Log\LoggerInterface;

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
        $namespace = is_string($namespace) && $namespace !== '' ? $namespace : 'Laravel/Queue';

        // Checked here so queue-monitor:status fails, rather than every sample.
        EmfDocument::assertNamespace($namespace);

        return new EmfSink($this->createEmitter(), $namespace, $this->entity());
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

        $log = $this->container->make(LogManager::class);
        $this->assertChannel($log, $channel);

        return new LogEmitter($log, $channel);
    }

    /**
     * LogManager falls back to the emergency logger for a channel it cannot
     * build, and a stack does the same for a member it cannot build. A level
     * above info, a buffering handler or a stack that ignores its failures
     * would drop documents just as quietly. Any of these would lose every
     * document while the counters are still drained.
     */
    private function assertChannel(LogManager $log, string $channel): void {
        $logger = $this->assertBuilt($log, $channel);
        $monolog = $logger instanceof Logger ? $logger->getLogger() : null;

        if ($monolog instanceof MonologLogger && ! self::delivers($monolog)) {
            throw new InvalidArgumentException(
                "The queue-monitor EMF log channel [{$channel}] must write info records straight through: "
                .'no level above info, no buffering or fingers-crossed handler, no ignore_exceptions.'
            );
        }
    }

    private function assertBuilt(LogManager $log, string $channel): LoggerInterface {
        $logger = $log->channel($channel);

        if (! array_key_exists($channel, $log->getChannels())) {
            throw new InvalidArgumentException("The queue-monitor EMF log channel [{$channel}] is not defined or could not be built.");
        }

        $config = $this->config->get("logging.channels.{$channel}");
        $members = is_array($config) && ($config['driver'] ?? null) === 'stack' ? $config['channels'] ?? [] : [];

        foreach (is_string($members) ? explode(',', $members) : (array) $members as $member) {
            if (is_string($member) && trim($member) !== '') {
                $this->assertBuilt($log, trim($member));
            }
        }

        return $logger;
    }

    /**
     * Walks the handlers the way Monolog does: the first one that takes an
     * info record decides, because a null handler stops it there.
     */
    private static function delivers(MonologLogger $logger): bool {
        $probe = new LogRecord(new DateTimeImmutable, $logger->getName(), Level::Info, '');

        foreach ($logger->getHandlers() as $handler) {
            if ($handler instanceof WhatFailureGroupHandler) {
                return false;
            }

            if ($handler->isHandling($probe)) {
                return ! $handler instanceof FingersCrossedHandler
                    && ! $handler instanceof BufferHandler
                    && ! $handler instanceof NullHandler;
            }
        }

        return false;
    }
}
