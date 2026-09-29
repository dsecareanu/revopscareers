<?php
// Called by daily_sync.sh via `wp eval-file` after every import run.
// Removes the featured flag from webadmin jobs older than 1 day. Jobs posted
// by any other author (paid client listings) stay featured until they expire.
// Set UNFEATURE_DRY_RUN=1 to preview without changing anything.
global $wpdb;

$webadmin_id = 1;
$max_days    = 1;
$dry_run     = getenv( 'UNFEATURE_DRY_RUN' ) === '1';
$cutoff      = gmdate( 'Y-m-d H:i:s', time() - $max_days * DAY_IN_SECONDS );

$featured = $wpdb->get_results(
	"SELECT p.ID, p.post_author, p.post_date_gmt, p.post_title
	 FROM {$wpdb->posts} p
	 JOIN {$wpdb->postmeta} m ON m.post_id = p.ID AND m.meta_key = '_featured' AND m.meta_value = '1'
	 WHERE p.post_type = 'job_listing' AND p.post_status = 'publish'
	 ORDER BY p.post_date_gmt"
);

$client = $fresh = $old = [];
foreach ( $featured as $job ) {
	if ( (int) $job->post_author !== $webadmin_id ) {
		$client[] = $job;
	} elseif ( $job->post_date_gmt < $cutoff ) {
		$old[] = $job;
	} else {
		$fresh[] = $job;
	}
}

echo 'Featured: ' . count( $featured ) . ' | client (keep): ' . count( $client )
	. ' | webadmin fresh (keep): ' . count( $fresh ) . ' | webadmin older than ' . $max_days . 'd (remove): ' . count( $old )
	. ( $dry_run ? ' [DRY RUN]' : '' ) . "\n";

foreach ( $client as $job ) {
	echo "  keep client job {$job->ID} (author {$job->post_author}, posted {$job->post_date_gmt} UTC): {$job->post_title}\n";
}

if ( $dry_run ) {
	return;
}

foreach ( $old as $job ) {
	// update_post_meta fires WP Job Manager's hooks: resets menu_order and flushes the listings cache.
	update_post_meta( $job->ID, '_featured', 0 );
}
echo 'Unfeatured ' . count( $old ) . " job(s).\n";
