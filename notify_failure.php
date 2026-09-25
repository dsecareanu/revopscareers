<?php
// Called by run_sync.sh via `wp eval-file` when a sync run fails.
$log  = getenv( 'SYNC_LOG' );
$tail = $log && is_readable( $log ) ? implode( '', array_slice( file( $log ), -60 ) ) : '(log not found)';

$sent = wp_mail(
	getenv( 'ALERT_EMAIL' ),
	'RevOpsCareers job sync failed (' . getenv( 'SYNC_MODE' ) . ' run)',
	"The job sync on the server exited with an error.\n\nFull log on the server: $log\n\nLast 60 lines:\n\n$tail"
);
echo $sent ? "Failure alert sent.\n" : "Failure alert could NOT be sent.\n";
