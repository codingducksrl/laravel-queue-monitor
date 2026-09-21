<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Sinks\Emf;

interface Emitter {
    public function emit(string $line): void;
}
