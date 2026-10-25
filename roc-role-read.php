
// ---------------------------------------------------------------------------
// Role read on featured RevOps job ads. Since 2026-10-25 sync_role_reads.py stores a ReLoop.me
// Quick Check (post meta _roc_reloop_read) and the page shows ReLoop's own embed. Older jobs may
// still have our Claude Haiku read (_roc_role_read, method: role_read/METHODOLOGY.md); it is shown
// only until the ReLoop read arrives.
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

    register_rest_route( 'roc/v1', '/role-read/(?P<id>\d+)', [
        'methods'             => 'POST',
        'permission_callback' => $can_edit,
        'callback'            => function ( WP_REST_Request $req ) {
            $id   = (int) $req['id'];
            $read = $req->get_json_params()['read'] ?? null;
            if ( get_post_type( $id ) !== 'job_listing' || ! is_array( $read ) ) {
                return new WP_Error( 'roc_bad_read', 'Unknown job or missing read', [ 'status' => 400 ] );
            }
            update_post_meta( $id, '_roc_role_read', wp_slash( wp_json_encode( $read ) ) );
            return [ 'ok' => true ];
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

function roc_role_read_html( $read ) {
    $e     = 'esc_html';
    $money = function ( $n ) { return '$' . number_format( $n / 1000 ) . 'k'; };
    $join  = function ( $items ) {   // "a, b and c"
        $items = array_values( array_filter( $items ) );
        return count( $items ) > 1 ? implode( ', ', array_slice( $items, 0, -1 ) ) . ' and ' . end( $items ) : ( $items[0] ?? '' );
    };
    $rl = 'https://www.reloop.me/role-intel?utm_source=revopscareers&utm_medium=job_page&utm_campaign=role_read';

    $people = [ 'yes' => 'manages people', 'no' => 'no people management', 'not stated' => 'no team stated' ][ $read['manages_people'] ] ?? '';
    $title  = [ 'matches' => 'the title matches the work', 'title overstates the work' => 'the title overstates the work',
                'title understates the work' => 'the title understates the work', 'unclear' => 'the title is hard to judge from the ad' ][ $read['title_match'] ] ?? '';
    $level  = 'Reads as a <strong>' . $e( $read['level'] ) . '</strong> role (' . $e( $people ) . '); ' . $e( $title ) . '. ' . $e( $read['level_reason'] );

    $pay = $read['pay'] ?? [];
    if ( ! empty( $pay['midpoint'] ) ) {
        $facts = 'Pay ' . $e( $pay['text'] ) . ' (median for US ' . $e( $read['level'] ) . ' ads here in 2026: ' . $money( $pay['typical_midpoint'] ) . ').';
    } elseif ( ! empty( $pay['stated'] ) ) {
        $text  = preg_match( '/^\s*[\d.,]+\s*$/', (string) $pay['text'] ) ? roc_role_read_salary_text( get_the_ID(), $pay['text'] ) : (string) $pay['text'];
        $facts = 'Pay is stated' . ( $text ? ': ' . $e( $text ) : '' ) . '.';
    } else {
        $facts = 'Pay is not stated.';
    }
    $years = $read['years'] ?? [];
    if ( ! empty( $years['asked'] ) ) {
        $facts .= ' Asks for ' . (int) $years['asked'] . '+ years (typical for this level: ' . (int) $years['typical'] . ').';
    }

    $musts = $join( array_map( function ( $i ) use ( $e ) { return $e( lcfirst( rtrim( $i['item'], '. ' ) ) ); }, array_slice( $read['must_haves'], 0, 3 ) ) );
    $extra = '';
    if ( ! empty( $read['checks'][0] ) ) $extra .= ' Worth checking: ' . $e( rtrim( $read['checks'][0]['note'], '. ' ) ) . '.';
    if ( ! empty( $read['questions'][0] ) ) $extra .= ' Ask: ' . $e( $read['questions'][0] );

    $h  = '<section class="roc-role-read" aria-label="Role read by RevOps Careers">';
    $h .= '<h3>Role read by RevOps Careers</h3>';
    $h .= '<p class="roc-rr-sub">following the <a href="https://www.reloop.me/charter?utm_source=revopscareers&amp;utm_medium=job_page&amp;utm_campaign=role_read" target="_blank" rel="noopener">ReLoop.me Fair Read Charter</a></p>';
    $h .= '<p>' . $e( $read['summary'] ) . '</p>';
    $h .= '<p>' . $level . ' ' . $facts . '</p>';
    if ( $musts || $extra ) $h .= '<p>' . ( $musts ? 'Must-haves: ' . $musts . '.' : '' ) . $extra . '</p>';
    $h .= '<p class="roc-rr-cta"><a href="' . esc_url( $rl ) . '" target="_blank" rel="noopener">'
        . 'Get the full read, with an estimated pay range: ReLoop.me Role Intel &rarr;</a></p>';
    $h .= '<p class="roc-rr-sub">AI-generated by RevOps Careers using Claude, '
        . $e( $read['generated_at'] ) . '. Indicative, not career advice. '
        . '<a href="mailto:hello@revopscareers.com?subject=' . rawurlencode( 'Role read: ' . get_the_title() ) . '">Report an issue</a>.</p>';
    $h .= '</section>';
    return $h;
}

add_filter( 'the_job_description', function ( $content ) {
    if ( ! is_singular( 'job_listing' ) ) return $content;
    // A ReLoop read replaces the Haiku one; ReLoop decides what to show (no embed = nothing shown)
    $reloop = json_decode( (string) get_post_meta( get_the_ID(), '_roc_reloop_read', true ), true );
    if ( is_array( $reloop ) ) {
        $embed = (string) ( $reloop['embed']['html'] ?? '' );
        if ( ! $embed ) return $content;
        $embed = str_replace( 'utm_source=quick_embed&amp;utm_medium=job_page&amp;utm_campaign=direct',
            'utm_source=revopscareers&amp;utm_medium=job_page&amp;utm_campaign=role_read', $embed );
        return $content . '<div class="roc-reloop-read">' . wp_kses_post( $embed ) . '</div>';
    }
    $read = json_decode( (string) get_post_meta( get_the_ID(), '_roc_role_read', true ), true );
    if ( ! is_array( $read ) || empty( $read['summary'] ) ) return $content;
    return $content . roc_role_read_html( $read );
}, 9 );

add_action( 'wp_head', function () {
    if ( ! is_singular( 'job_listing' ) ) return;
    echo '<style>
.roc-role-read{border:1px solid #e3e6ea;border-left:4px solid #f16736;border-radius:6px;padding:18px 22px 10px;margin:28px 0 20px;background:#fafbfc}
.roc-role-read h3{margin:0 0 8px}.roc-role-read p{margin:0 0 10px}
.roc-rr-cta{font-weight:600}.roc-rr-sub{font-size:.8rem;color:#5f6b7a}
.roc-reloop-read{margin:28px 0 20px}
</style>';
} );
