# Laravel Queue Monitor

Queue monitoring for Laravel applications.

## Requirements

- PHP 8.3+
- Laravel 13

## Installation

```bash
composer require codingducksrl/laravel-queue-monitor
```

Publish the config if you need to change the defaults:

```bash
php artisan vendor:publish --tag=queue-monitor-config
```

## Configuration

`config/queue-monitor.php`:

| Key           | Default | Meaning                                                            |
|---------------|---------|--------------------------------------------------------------------|
| `enabled`     | `true`  | Master switch, also bound to `QUEUE_MONITOR_ENABLED`.               |
| `connections` | `[]`    | Queue connections to monitor. Empty means every connection.         |

## Usage

```php
use CodingDuck\QueueMonitor\Facades\QueueMonitor;

QueueMonitor::enabled();          // bool
QueueMonitor::connections();      // list<string>
QueueMonitor::monitors('redis');  // bool
```

Inspect the effective configuration from the console:

```bash
php artisan queue-monitor:status
```

## Development

```bash
composer install
composer test          # analyse + lint:check + type coverage + unit
composer analyse       # PHPStan / Larastan, level 10
composer lint          # Pint, fix in place
composer lint:check    # Pint, dry run
composer test:unit     # Pest
```

### Sail

The repository ships a Sail environment so the whole toolchain can run in Docker.
`laravel.test` is an idle container (`sleep infinity`) rather than a web server,
because this is a package and not an application — you exec into it rather than
browsing it.

```bash
./vendor/bin/sail up -d
```

Every script above works through Sail, plus the binaries directly:

```bash
sail composer test                          # the full pipeline
sail composer analyse                       # Larastan, level 10
sail pint                                   # Pint, fix in place
sail pest                                   # Pest
sail bin testbench queue-monitor:status     # package Artisan commands
sail shell                                  # a shell in the container
```

Two backends come up alongside it, reachable from the container as `pgsql` and
`redis`, and from the host on ports `5433` and `6380` (both overridable with
`FORWARD_DB_PORT` and `FORWARD_REDIS_PORT`, so they do not collide with other
Sail projects):

```bash
sail psql                                   # database `testing`, user `sail`
sail redis                                  # redis-cli
```

The package's own tests use Testbench's SQLite connection, so `composer test`
does not need either service.

The container also carries Traefik labels for `queue-monitor.app.localhost`,
matching the other Coding Duck projects; they only take effect if a Traefik
instance is attached to the external `proxy` network.

### Workbench

The package ships a Testbench workbench for manual exercise:

```bash
composer build
composer serve
```

## License

MIT. See [LICENSE.md](LICENSE.md).
