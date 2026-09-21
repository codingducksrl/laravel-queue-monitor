<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Sinks\Emf;

use RuntimeException;
use SplFileObject;

final class StdoutEmitter implements Emitter {
    public function __construct(private ?SplFileObject $handle = null) {}

    public function emit(string $line): void {
        $handle = $this->handle ??= new SplFileObject('php://stdout', 'wb');

        if ($handle->fwrite($line."\n") === false) {
            throw new RuntimeException('Unable to write an EMF document to the output stream.');
        }
    }
}
