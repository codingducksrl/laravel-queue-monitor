<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Sinks\Emf\LogEmitter;
use Illuminate\Log\LogManager;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;

it('writes the document as a bare JSON line through the documented channel', function (): void {
    $path = tempnam(sys_get_temp_dir(), 'emf');

    config()->set('logging.channels.emf', [
        'driver' => 'monolog',
        'handler' => StreamHandler::class,
        'handler_with' => ['stream' => $path],
        'formatter' => LineFormatter::class,
        'formatter_with' => ['format' => "%message%\n", 'ignoreEmptyContextAndExtra' => true],
    ]);

    (new LogEmitter(app(LogManager::class), 'emf'))->emit('{"_aws":{"Timestamp":1}}');

    expect(trim((string) file_get_contents($path)))->toBe('{"_aws":{"Timestamp":1}}');

    unlink($path);
});

it('shows why a channel without an explicit formatter cannot be used', function (): void {
    $path = tempnam(sys_get_temp_dir(), 'emf');

    config()->set('logging.channels.decorating', [
        'driver' => 'monolog',
        'handler' => StreamHandler::class,
        'handler_with' => ['stream' => $path],
    ]);

    (new LogEmitter(app(LogManager::class), 'decorating'))->emit('{"_aws":{"Timestamp":1}}');

    // Laravel installs its own LineFormatter when the key is absent, and the
    // prefix it adds makes the line unparseable as EMF.
    expect(trim((string) file_get_contents($path)))
        ->not->toStartWith('{')
        ->toContain('testing.INFO: {"_aws"');

    unlink($path);
});
