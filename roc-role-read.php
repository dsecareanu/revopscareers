
// ---------------------------------------------------------------------------
// Role read: candidate-focused read of featured RevOps job ads (post meta _roc_role_read),
// written by sync_role_reads.py (Claude Haiku 5.5). Method: role_read/METHODOLOGY.md.
// Shown under the job description, above "Tagged as" (the tags plugin appends at priority 10).
// ---------------------------------------------------------------------------
add_action( 'rest_api_init', function () {
    $can_edit = function () { return current_user_can( 'edit_posts' ); };

    // Featured, published Revenue Operations (cat 21) jobs without a read yet, newest first
    // (?days=N: all cat-21 jobs from the last N days, for backfills)
    register_rest_route( 'roc/v1', '/role-read-queue', [
        'methods'             => 'GET',
        'permission_callback' => $can_edit,
        'callback'            => function ( WP_REST_Request $req ) {
            $limit = min( 500, max( 1, (int) ( $req['limit'] ?? 60 ) ) );
            $days  = (int) ( $req['days'] ?? 0 );   // backfill: all cat-21 jobs from the last N days
            $meta  = [ [ 'key' => '_roc_role_read', 'compare' => 'NOT EXISTS' ] ];
            if ( ! $days ) $meta[] = [ 'key' => '_featured', 'value' => '1' ];
            $ids   = get_posts( [
                'post_type'      => 'job_listing',
                'post_status'    => 'publish',
                'posts_per_page' => $limit,
                'fields'         => 'ids',
                'orderby'        => 'date',
                'order'          => 'DESC',
                'tax_query'      => [ [ 'taxonomy' => 'job_listing_category', 'terms' => [ 21 ] ] ],
                'meta_query'     => $meta,
                'date_query'     => $days ? [ [ 'after' => "$days days ago", 'inclusive' => true ] ] : [],
            ] );
            return array_map( function ( $id ) {
                return [
                    'id'       => $id,
                    'title'    => get_the_title( $id ),
                    'location' => (string) get_post_meta( $id, '_job_location', true ),
                    'salary'   => (string) get_post_meta( $id, '_job_salary', true ),
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
} );

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
        $facts = 'Pay is stated' . ( $pay['text'] ? ': ' . $e( $pay['text'] ) : '' ) . '.';
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

    $h  = '<section class="roc-role-read" aria-label="Role read">';
    $h .= '<h3>Role read</h3>';
    $h .= '<p>' . $e( $read['summary'] ) . '</p>';
    $h .= '<p>' . $level . ' ' . $facts . '</p>';
    if ( $musts || $extra ) $h .= '<p>' . ( $musts ? 'Must-haves: ' . $musts . '.' : '' ) . $extra . '</p>';
    $h .= '<p class="roc-rr-cta"><a href="' . esc_url( $rl ) . '" target="_blank" rel="noopener">'
        . 'Get the full read, with an estimated pay range: ReLoop.me Role Intel &rarr;</a></p>';
    $h .= '<p class="roc-rr-sub">AI-generated by RevOps Careers using Claude, based on '
        . '<a href="https://www.reloop.me/charter" target="_blank" rel="noopener">ReLoop.me&rsquo;s Fair Read method</a>, '
        . $e( $read['generated_at'] ) . '. Indicative, not career advice. '
        . '<a href="mailto:hello@revopscareers.com?subject=' . rawurlencode( 'Role read: ' . get_the_title() ) . '">Report an issue</a>.</p>';
    $h .= '</section>';
    return $h;
}

add_filter( 'the_job_description', function ( $content ) {
    if ( ! is_singular( 'job_listing' ) ) return $content;
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
</style>';
} );
