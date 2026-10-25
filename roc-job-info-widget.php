
// ---------------------------------------------------------------------------
// Job Information box in the single job sidebar, under Company Information
// (the theme prints that at ignition_sidebar_before priority 20).
// ---------------------------------------------------------------------------
add_action( 'ignition_sidebar_before', function () {
    if ( ! is_singular( 'job_listing' ) ) return;
    $id    = get_the_ID();
    $terms = function ( $tax ) use ( $id ) {
        $t = get_the_terms( $id, $tax );
        if ( ! $t || is_wp_error( $t ) ) return '';
        return implode( ', ', array_map( fn( $x ) => '<a href="' . esc_url( get_term_link( $x ) ) . '">' . esc_html( $x->name ) . '</a>', $t ) );
    };
    $salary = trim( (string) get_post_meta( $id, '_job_salary', true ) );
    if ( $salary !== '' && preg_match( '/^\s*[\d.,]+\s*$/', $salary ) ) $salary = roc_role_read_salary_text( $id, $salary );
    $location = trim( (string) get_post_meta( $id, '_job_location', true ) );
    if ( get_post_meta( $id, '_remote_position', true ) && stripos( $location, 'remote' ) === false ) {
        $location = $location ? $location . ' (remote)' : 'Remote';
    }
    $expires = (string) get_post_meta( $id, '_job_expires', true );
    $rows = array_filter( [
        'Category'    => $terms( 'job_listing_category' ),
        'Job type'    => $terms( 'job_listing_type' ),
        'Location'    => esc_html( $location ),
        'Salary'      => $salary !== '' ? esc_html( $salary ) : 'Not stated',
        'Posted'      => esc_html( get_the_date( 'j M Y', $id ) ),
        'Open until'  => $expires ? esc_html( date_i18n( 'j M Y', strtotime( $expires ) ) ) : '',
    ] );
    echo '<aside class="widget widget_roc-job-info"><h3 class="widget-title">Job Information</h3><dl class="roc-job-info">';
    foreach ( $rows as $label => $value ) {
        echo '<dt>' . esc_html( $label ) . '</dt><dd>' . $value . '</dd>';
    }
    echo '</dl></aside>';
}, 21 );

add_action( 'wp_head', function () {
    if ( ! is_singular( 'job_listing' ) ) return;
    echo '<style>.roc-job-info{margin:0}.roc-job-info dt{font-size:.8rem;text-transform:uppercase;letter-spacing:.04em;color:#5f6b7a;margin-top:10px}.roc-job-info dt:first-child{margin-top:0}.roc-job-info dd{margin:2px 0 0}</style>';
} );
