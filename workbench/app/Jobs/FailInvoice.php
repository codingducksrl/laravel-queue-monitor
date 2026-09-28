<?php

declare(strict_types=1);

namespace Workbench\App\Jobs;

use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use RuntimeException;

class FailInvoice implements ShouldQueue {
    use Queueable;

    public int $tries = 1;

    public function handle(): void {
        throw new RuntimeException('The invoice could not be sent.');
    }
}
