<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Sinks\Emf;

use RuntimeException;
use SplFileObject;

final class StdoutEmitter implements Emitter {
    public function __construct(private ?SplFileObject $handle = null) {}

    public function emit(string $line): void {
        $handle = $this->handle ??= new SplFileObject('php://stdout', 'wb');

        if (self::discards($handle)) {
            throw new RuntimeException(
                'The EMF output stream is /dev/null, so every document would be discarded. The scheduler sends '
                .'a command\'s output there unless told otherwise: add ->appendOutputTo(\'/proc/1/fd/1\') or set queue-monitor.emf.channel.'
            );
        }

        $line .= "\n";

        if ($handle->fwrite($line) !== strlen($line)) {
            throw new RuntimeException('Unable to write an EMF document to the output stream.');
        }
    }

    /**
     * Writing to /dev/null succeeds, so without this the sampler would drain
     * counters that nobody ever receives.
     */
    private static function discards(SplFileObject $handle): bool {
        $null = @stat('/dev/null');
        $out = $handle->fstat();

        return $null !== false && $out['dev'] === $null['dev'] && $out['ino'] === $null['ino'];
    }
}
