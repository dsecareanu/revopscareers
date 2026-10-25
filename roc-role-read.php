
// ---------------------------------------------------------------------------
// Role read on featured RevOps job ads. Since 2026-10-25 sync_role_reads.py stores a ReLoop.me
// Quick Check (post meta _roc_reloop_read) and the page shows ReLoop's own embed. Our earlier Claude
// Haiku reads (_roc_role_read, method: role_read/METHODOLOGY.md) are no longer shown, at ReLoop's
// request; the queue uses them to find jobs that still need a ReLoop read.
// Shown under the job description, above "Tagged as" (the tags plugin appends at priority 10).
// ---------------------------------------------------------------------------
add_action( 'rest_api_init', function () {
    $can_edit = function () { return current_user_can( 'edit_posts' ); };

    // Published Revenue Operations (cat 21) jobs without a ReLoop read, newest first:
    // featured ones, plus older ones that still show a Haiku read
    register_rest_route( 'roc/v1', '/role-read-queue', [
        'methods'             => 'GET',
        'permission_callback' => $can_edit,
        'callback'            => function ( WP_REST_Request $req ) {
            $limit = min( 500, max( 1, (int) ( $req['limit'] ?? 50 ) ) );
            $meta  = [
                [ 'key' => '_roc_reloop_read', 'compare' => 'NOT EXISTS' ],
                [ 'relation' => 'OR',
                  [ 'key' => '_featured', 'value' => '1' ],
                  [ 'key' => '_roc_role_read', 'value' => '"summary"', 'compare' => 'LIKE' ] ],
            ];
            $ids   = get_posts( [
                'post_type'      => 'job_listing',
                'post_status'    => 'publish',
                'posts_per_page' => $limit,
                'fields'         => 'ids',
                'orderby'        => 'date',
                'order'          => 'DESC',
                'tax_query'      => [ [ 'taxonomy' => 'job_listing_category', 'terms' => [ 21 ] ] ],
                'meta_query'     => $meta,
            ] );
            return array_map( function ( $id ) {
                return [
                    'id'       => $id,
                    'title'    => get_the_title( $id ),
                    'content'  => get_post_field( 'post_content', $id ),
                ];
            }, $ids );
        },
    ] );

    register_rest_route( 'roc/v1', '/reloop-read/(?P<id>\d+)', [
        'methods'             => 'POST',
        'permission_callback' => $can_edit,
        'callback'            => function ( WP_REST_Request $req ) {
            $id   = (int) $req['id'];
            $read = $req->get_json_params()['read'] ?? null;
            if ( get_post_type( $id ) !== 'job_listing' || ! is_array( $read ) ) {
                return new WP_Error( 'roc_bad_read', 'Unknown job or missing read', [ 'status' => 400 ] );
            }
            update_post_meta( $id, '_roc_reloop_read', wp_slash( wp_json_encode( $read ) ) );
            return [ 'ok' => true ];
        },
    ] );
} );

/**
 * Salary field value ("138000.0") as display text: currency from _job_salary_currency, else from the
 * job's country, else $; whole number; period from _job_salary_unit, or "a year" for amounts >= 20,000.
 */
function roc_role_read_salary_text( $post_id, $raw ) {
    $amount = (float) str_replace( ',', '', $raw );
    if ( $amount <= 0 ) return '';
    $code = strtoupper( (string) get_post_meta( $post_id, '_job_salary_currency', true ) );
    if ( ! $code ) {
        $by_country = [ 'United States' => 'USD', 'Canada' => 'CAD', 'United Kingdom' => 'GBP', 'Singapore' => 'SGD',
            'Australia' => 'AUD', 'New Zealand' => 'NZD', 'India' => 'INR', 'Switzerland' => 'CHF', 'Japan' => 'JPY',
            'Germany' => 'EUR', 'France' => 'EUR', 'Spain' => 'EUR', 'Italy' => 'EUR', 'Netherlands' => 'EUR', 'Ireland' => 'EUR',
            'Belgium' => 'EUR', 'Austria' => 'EUR', 'Portugal' => 'EUR', 'Finland' => 'EUR', 'Poland' => 'PLN', 'Sweden' => 'SEK' ];
        $code = $by_country[ (string) roc_extract_country( (string) get_post_meta( $post_id, '_job_location', true ) ) ] ?? 'USD';
    }
    $symbols = [ 'USD' => '$', 'EUR' => '€', 'GBP' => '£', 'SGD' => 'S$', 'CAD' => 'C$', 'AUD' => 'A$', 'NZD' => 'NZ$', 'INR' => '₹', 'JPY' => '¥' ];
    $money   = isset( $symbols[ $code ] ) ? $symbols[ $code ] . number_format( $amount ) : $code . ' ' . number_format( $amount );
    $unit    = strtoupper( (string) get_post_meta( $post_id, '_job_salary_unit', true ) );
    $period  = [ 'YEAR' => 'a year', 'MONTH' => 'a month', 'WEEK' => 'a week', 'DAY' => 'a day', 'HOUR' => 'an hour' ][ $unit ]
        ?? ( $amount >= 20000 ? 'a year' : '' );
    return trim( $money . ' ' . $period );
}

add_filter( 'the_job_description', function ( $content ) {
    if ( ! is_singular( 'job_listing' ) ) return $content;
    $reloop = json_decode( (string) get_post_meta( get_the_ID(), '_roc_reloop_read', true ), true );
    $embed  = (string) ( $reloop['embed']['html'] ?? '' );   // ReLoop decides what to show (no embed = nothing shown)
    if ( ! $embed ) return $content;
    $embed = str_replace( 'utm_source=quick_embed&amp;utm_medium=job_page&amp;utm_campaign=direct',
        'utm_source=revopscareers&amp;utm_medium=job_page&amp;utm_campaign=role_read', $embed );
    return $content . '<div class="roc-reloop-read">' . wp_kses_post( $embed ) . '</div>';
}, 9 );

add_action( 'wp_head', function () {
    if ( ! is_singular( 'job_listing' ) ) return;
    echo '<style>
.roc-reloop-read{margin:28px 0 20px}
</style>';
} );
