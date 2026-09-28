<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor\Tests\Support;

use Closure;
use Illuminate\Cache\ArrayStore;

/**
 * Records every store call the code under test makes, one per round trip,
 * ignoring the calls ArrayStore makes to itself.
 */
final class SpyStore extends ArrayStore {
    /** @var list<string> */
    public array $calls = [];

    private int $depth = 0;

    public function get($key) {
        return $this->spy(__FUNCTION__, fn (): mixed => parent::get($key));
    }

    public function many(array $keys) {
        return $this->spy(__FUNCTION__, fn (): mixed => parent::many($keys));
    }

    public function put($key, $value, $seconds) {
        return $this->spy(__FUNCTION__, fn (): mixed => parent::put($key, $value, $seconds));
    }

    public function increment($key, $value = 1) {
        return $this->spy(__FUNCTION__, fn (): mixed => parent::increment($key, $value));
    }

    public function decrement($key, $value = 1) {
        return $this->spy(__FUNCTION__, fn (): mixed => parent::decrement($key, $value));
    }

    public function forever($key, $value) {
        return $this->spy(__FUNCTION__, fn (): mixed => parent::forever($key, $value));
    }

    public function touch($key, $seconds) {
        return $this->spy(__FUNCTION__, fn (): mixed => parent::touch($key, $seconds));
    }

    public function forget($key) {
        return $this->spy(__FUNCTION__, fn (): mixed => parent::forget($key));
    }

    public function lock($name, $seconds = 0, $owner = null) {
        return $this->spy(__FUNCTION__, fn (): mixed => parent::lock($name, $seconds, $owner));
    }

    private function spy(string $method, Closure $call): mixed {
        if ($this->depth === 0) {
            $this->calls[] = $method;
        }

        $this->depth++;

        try {
            return $call();
        } finally {
            $this->depth--;
        }
    }
}
