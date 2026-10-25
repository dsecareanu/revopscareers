
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
    $e    = 'esc_html';
    $list = function ( $items, $quoted = true ) use ( $e ) {
        $out = '';
        foreach ( $items as $i ) {
            $out .= $quoted
                ? '<li>' . $e( $i['item'] ) . ' <span class="roc-rr-quote">&ldquo;' . $e( $i['quote'] ) . '&rdquo;</span></li>'
                : '<li>' . $e( $i ) . '</li>';
        }
        return $out ? '<ul>' . $out . '</ul>' : '';
    };
    $axes = [
        'hands-on vs strategic'     => [ 'Hands-on', 'Strategic' ],
        'maintain vs build'         => [ 'Maintain', 'Build' ],
        'specialist vs generalist'  => [ 'Specialist', 'Generalist' ],
        'clear vs open-ended scope' => [ 'Clear scope', 'Open-ended scope' ],
    ];
    $manages = [ 'yes' => 'Yes', 'no' => 'No', 'not stated' => 'Not stated' ][ $read['manages_people'] ] ?? 'Not stated';
    $rl      = 'https://www.reloop.me/role-intel?utm_source=revopscareers&utm_medium=job_page&utm_campaign=role_read';
    $money   = function ( $n ) { return '$' . number_format( $n / 1000 ) . 'k'; };

    $pay = $read['pay'] ?? [];
    if ( ! empty( $pay['midpoint'] ) ) {
        $pay_line = 'Stated: ' . $e( $pay['text'] ) . '. The median for US ' . $e( $read['level'] )
            . ' ads on RevOps Careers in 2026 is ' . $money( $pay['typical_midpoint'] ) . '.';
    } elseif ( ! empty( $pay['stated'] ) ) {
        $pay_line = 'Stated' . ( $pay['text'] ? ': ' . $e( $pay['text'] ) : '' ) . '.';
    } else {
        $pay_line = 'Not stated in the ad.';
    }
    $years      = $read['years'] ?? [];
    $years_line = ! empty( $years['asked'] )
        ? 'Asks for ' . (int) $years['asked'] . '+ years; typical for this level on RevOps Careers: ' . (int) $years['typical'] . '.'
        : 'Not stated in the ad.';

    $h  = '<section class="roc-role-read" aria-label="Role read">';
    $h .= '<h3>Role read</h3>';
    $h .= '<p class="roc-rr-sub">AI-generated read of this job ad by RevOps Careers, using Claude, based on '
        . '<a href="https://www.reloop.me/charter" target="_blank" rel="noopener">ReLoop.me&rsquo;s Fair Read method</a>. '
        . 'Indicative, not career advice.</p>';
    $h .= '<p><strong>In plain words:</strong> ' . $e( $read['summary'] ) . '</p>';
    $h .= '<p class="roc-rr-facts"><span>Level: <strong>' . $e( $read['level'] ) . '</strong></span>'
        . '<span>Manages people: <strong>' . $manages . '</strong></span>'
        . '<span>Title: <strong>' . $e( ucfirst( $read['title_match'] ) ) . '</strong></span></p>';
    $h .= '<p class="roc-rr-why">' . $e( $read['level_reason'] ) . '</p>';
    $h .= '<p><strong>Pay:</strong> ' . $pay_line . ' <strong>Experience:</strong> ' . $years_line . '</p>';
    if ( $read['must_haves'] )    $h .= '<h4>Must-haves</h4>' . $list( $read['must_haves'] );
    if ( $read['nice_to_haves'] ) $h .= '<h4>Nice to have</h4>' . $list( $read['nice_to_haves'] );
    if ( $read['checks'] ) {
        $h .= '<h4>Worth checking before you apply</h4><ul>';
        foreach ( $read['checks'] as $c ) {
            $h .= '<li><strong>' . $e( ucfirst( $c['flag'] ) ) . ':</strong> ' . $e( $c['note'] )
                . ' <span class="roc-rr-quote">&ldquo;' . $e( $c['quote'] ) . '&rdquo;</span></li>';
        }
        $h .= '</ul>';
    }
    $h .= '<details><summary>How the job leans, AI in the role, and questions to ask</summary>';
    if ( $read['leans'] ) {
        $h .= '<h4>How the job leans</h4><ul>';
        foreach ( $read['leans'] as $l ) {
            [ $a, $b ] = $axes[ $l['axis'] ] ?? [ '', '' ];
            $lean = [ 'mostly the first' => "Mostly $a", 'balanced' => "Balanced $a / $b", 'mostly the second' => "Mostly $b" ][ $l['lean'] ] ?? '';
            $h   .= '<li><strong>' . $e( $lean ) . ':</strong> ' . $e( $l['reason'] ) . '</li>';
        }
        $h .= '</ul>';
    }
    $h .= '<h4>AI in the role</h4><p>' . $e( ucfirst( $read['ai_in_role'] ) )
        . ( $read['ai_quote'] ? ' <span class="roc-rr-quote">&ldquo;' . $e( $read['ai_quote'] ) . '&rdquo;</span>' : '' ) . '</p>';
    if ( $read['apply_if'] )  $h .= '<h4>Worth applying if</h4>' . $list( $read['apply_if'], false );
    if ( $read['questions'] ) $h .= '<h4>Questions to ask</h4>' . $list( $read['questions'], false );
    $h .= '</details>';
    $h .= '<p class="roc-rr-cta"><a href="' . esc_url( $rl ) . '" target="_blank" rel="noopener">'
        . 'Want the full read, with an estimated pay range? Get a ReLoop.me Role Intel &rarr;</a></p>';
    $h .= '<p class="roc-rr-foot">Generated ' . $e( $read['generated_at'] ) . '. Something looks wrong? '
        . '<a href="mailto:hello@revopscareers.com?subject=' . rawurlencode( 'Role read: ' . get_the_title() ) . '">Tell us</a>.</p>';
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
.roc-role-read{border:1px solid #e3e6ea;border-left:4px solid #f16736;border-radius:6px;padding:20px 22px;margin:28px 0 20px;background:#fafbfc}
.roc-role-read h3{margin:0 0 4px}.roc-role-read h4{margin:16px 0 6px;font-size:1rem}
.roc-rr-sub,.roc-rr-foot,.roc-rr-why{font-size:.85rem;color:#5f6b7a}.roc-rr-quote{color:#5f6b7a;font-style:italic}
.roc-rr-facts span{display:inline-block;margin:0 18px 6px 0}.roc-role-read ul{margin:0 0 8px 18px}
.roc-role-read details{margin-top:14px}.roc-role-read summary{cursor:pointer;font-weight:600}
.roc-rr-cta{margin:16px 0 4px;font-weight:600}
</style>';
} );
