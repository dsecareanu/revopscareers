<?php
/**
 * Collapse published job_listing posts that share the same company + title
 * (the same job posted in several cities) into one post.
 *
 * Keeps the newest post of each group (latest expiry, freshest apply link) — or
 * the client post if the group has one — 301-redirects the others to it via the
 * Redirection plugin, then deletes them. Only webadmin (author 1) posts are deleted.
 *
 *   wp eval-file collapse_duplicate_jobs.php            # dry run
 *   COLLAPSE_LIVE=1 wp eval-file collapse_duplicate_jobs.php
 *
 * Writes deleted IDs to ~/logs/collapse_deleted_ids.txt (one per line).
 */

$live  = getenv('COLLAPSE_LIVE') === '1';
$batch = 200;   // deletes between pauses
$pause = 2;     // seconds

function collapse_key($s) {
    $s = mb_strtolower(trim(html_entity_decode($s, ENT_QUOTES | ENT_HTML5, 'UTF-8')));
    $s = preg_replace('/[^\p{L}\p{N}_\s]/u', '', $s);
    return preg_replace('/\s+/u', ' ', $s);
}

global $wpdb;
$rows = $wpdb->get_results("
    SELECT p.ID, p.post_title, p.post_author, p.post_date_gmt, m.meta_value AS company
    FROM {$wpdb->posts} p
    JOIN {$wpdb->postmeta} m ON m.post_id = p.ID AND m.meta_key = '_company_name'
    WHERE p.post_type = 'job_listing' AND p.post_status = 'publish'
");

$groups = [];
foreach ($rows as $r) {
    $c = collapse_key($r->company);
    $t = collapse_key($r->post_title);
    if ($c === '' || $t === '') continue;
    $groups["$c|$t"][] = $r;
}

$plan = [];   // [keep_row, [dupe_rows]]
foreach ($groups as $g) {
    if (count($g) < 2) continue;
    usort($g, function ($a, $b) {
        $ca = (int) $a->post_author !== 1;   // client posts first
        $cb = (int) $b->post_author !== 1;
        if ($ca !== $cb) return $cb <=> $ca;
        return strcmp($b->post_date_gmt, $a->post_date_gmt) ?: ($b->ID <=> $a->ID);  // newest first
    });
    $keep  = array_shift($g);
    $dupes = array_values(array_filter($g, fn($r) => (int) $r->post_author === 1));
    if ($dupes) $plan[] = [$keep, $dupes];
}

$total = array_sum(array_map(fn($p) => count($p[1]), $plan));
WP_CLI::log(sprintf("%d published jobs, %d groups with duplicates, %d posts to delete.", count($rows), count($plan), $total));
foreach (array_slice($plan, 0, 5) as [$keep, $dupes]) {
    WP_CLI::log("  KEEP {$keep->ID} [{$keep->post_date_gmt}] {$keep->post_title} @ {$keep->company} — drop " . count($dupes));
}
if (!$live) {
    WP_CLI::log('Dry run — set COLLAPSE_LIVE=1 to apply.');
    return;
}

wp_defer_term_counting(true);
$log  = fopen(getenv('HOME') . '/logs/collapse_deleted_ids.txt', 'a');
$home = home_url();
$done = $errors = 0;

foreach ($plan as [$keep, $dupes]) {
    $target = get_permalink($keep->ID);
    foreach ($dupes as $d) {
        $path = wp_make_link_relative(get_permalink($d->ID));
        $item = Red_Item::create([
            'url'         => $path,
            'action_data' => ['url' => $target],
            'action_type' => 'url',
            'action_code' => 301,
            'match_type'  => 'url',
            'group_id'    => 1,
            'regex'       => false,
        ]);
        if (is_wp_error($item)) {
            $errors++;
            WP_CLI::warning("Redirect failed for {$d->ID} ($path): " . $item->get_error_message() . ' — not deleted');
            continue;
        }
        if (!wp_delete_post($d->ID, true)) {
            $errors++;
            WP_CLI::warning("Delete failed for {$d->ID}");
            continue;
        }
        fwrite($log, $d->ID . "\n");
        if (++$done % $batch === 0) {
            WP_CLI::log("  $done / $total deleted");
            wp_cache_flush_runtime();
            sleep($pause);
        }
    }
}
fclose($log);
wp_defer_term_counting(false);
WP_CLI::success("Deleted $done duplicate posts with redirects ($errors errors).");
