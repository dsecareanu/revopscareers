<?php
add_action(
	'after_setup_theme',
	function() {
		add_theme_support( 'hivepress' );
	}
);
add_action( 'after_setup_theme', 'ignition_specialty_child_before_setup', 5 );
function ignition_specialty_child_before_setup() {

	/*
	 * Loads the language files of the child theme (if any).
	 * If you use any of the WordPress localization functions, you will need to provide the same text domain
	 * exactly as the one mentioned below, i.e. 'ignition-specialty-child'
	 * If you change this value, then you'll need to modify your function calls accordingly.
	 * Localization functions reference: https://codex.wordpress.org/L10n
	 */
	load_child_theme_textdomain( 'ignition-specialty-child', get_stylesheet_directory() . '/languages' );

	/*
	 * Code added here will execute BEFORE the respective ignition_specialty_setup() function of the parent theme.
	 */


} //ignition_specialty_child_before_setup


add_action( 'after_setup_theme', 'ignition_specialty_child_after_setup', 15 );
function ignition_specialty_child_after_setup() {

	/*
	 * Code added here will execute AFTER the respective ignition_specialty_setup() function of the parent theme.
	 */


} //ignition_specialty_child_after_setup


add_action( 'wp_enqueue_scripts', 'ignition_specialty_child_enqueue_scripts', 15 );
function ignition_specialty_child_enqueue_scripts() {
	$theme = wp_get_theme();

	/*
	 * Code added here will execute AFTER the parent theme's scripts/styles have been enqueued,
	 * but haven't been printed yet.
	 */

	/*
	 * The parent theme's stylesheet, as well as this child's stylesheet, are loaded by default by the
	 * parent theme itself. This means you can easily add you CSS customizations by just adding them
	 * inside this child's style.css file.
	 */

	/*
	 * If you DON'T want the parent stylesheet to load, then uncomment the following line.
	 */
	//wp_dequeue_style( 'ignition-specialty-style' );


	/*
	 * Note that the wp_dequeue_style() call above, does not dequeue the rest of the stylesheets.
	 * If you need the rest of the stylesheets not to load, then uncomment the following lines:
	 */
	//wp_dequeue_style( 'ignition-specialty-main-before' );
	//wp_dequeue_style( 'ignition-specialty-main-after' );

} //ignition_specialty_child_enqueue_scripts

/*
 * In order to dequeue a specific parent script/style, inspect the theme's inc/scripts-styles.php and locate its
 * registration, and which variable it's added. Depending on the variable, it can be filtered via the following filters:
 * $styles_before  -> 'ignition_specialty_styles_before_main'
 * $styles_after   -> 'ignition_specialty_styles_after_main'
 * $scripts_before -> 'ignition_specialty_scripts_before_main'
 * $scripts_after  -> 'ignition_specialty_scripts_after_main'
 *
 * Once you locate the appropriate variable/filter, you can then hook and filter the array of script/style handles.
 * For example, the following filter removes 'ignition-specialty-woocommerce' from the $styles_after array.
 */
//add_filter( 'ignition_specialty_styles_after_main', function( $styles ) {
//	$found = array_search( 'ignition-specialty-woocommerce', $styles, true );
//	if ( false !== $found ) {
//		unset( $styles[ $found ] );
//	}
//
//	return $styles;
//} );


/**
 * RevOpsCareers – /wp-json/roc/v1/countries endpoint
 *
 * Add this snippet to your theme's functions.php (or a Code Snippets plugin).
 * Returns countries aggregated from published job_listing posts, sorted by
 * job count descending (Remote always last).
 *
 * Results are cached for 1 hour via a transient; the cache is automatically
 * busted whenever a job listing is saved, published, or deleted.
 */

// ---------------------------------------------------------------------------
// 1. Register the REST route
// ---------------------------------------------------------------------------
add_action( 'rest_api_init', function () {
    register_rest_route( 'roc/v1', '/countries', [
        'methods'             => 'GET',
        'callback'            => 'roc_countries_endpoint',
        'permission_callback' => '__return_true',
    ] );
} );

// ---------------------------------------------------------------------------
// 2. Bust the cache on any job listing change
// ---------------------------------------------------------------------------
add_action( 'save_post_job_listing',   'roc_countries_bust_cache' );
add_action( 'trash_post',              'roc_countries_bust_cache' );
add_action( 'delete_post',             'roc_countries_bust_cache' );

function roc_countries_bust_cache() {
    delete_transient( 'roc_countries_v1' );
}

// ---------------------------------------------------------------------------
// 3. Endpoint callback
// ---------------------------------------------------------------------------
function roc_countries_endpoint() {
    $cached = get_transient( 'roc_countries_v1' );
    // Invalidate cache if it pre-dates the slug/url fields
    if ( false !== $cached && isset( $cached[0] ) && ! isset( $cached[0]['slug'] ) ) {
        $cached = false;
    }
    if ( false !== $cached ) {
        return rest_ensure_response( $cached );
    }

    global $wpdb;

    // Single efficient query — grab location + remote flag for every published job
    $rows = $wpdb->get_results( "
        SELECT
            COALESCE(pm_loc.meta_value, '')    AS location,
            COALESCE(pm_rem.meta_value, '0')   AS remote
        FROM {$wpdb->posts} p
        LEFT JOIN {$wpdb->postmeta} pm_loc
            ON  pm_loc.post_id   = p.ID
            AND pm_loc.meta_key  = '_job_location'
        LEFT JOIN {$wpdb->postmeta} pm_rem
            ON  pm_rem.post_id   = p.ID
            AND pm_rem.meta_key  = '_remote_position'
        WHERE p.post_type   = 'job_listing'
          AND p.post_status  = 'publish'
    ", ARRAY_A );

    $counts = [];

    foreach ( $rows as $row ) {
        $location = trim( $row['location'] );
        $remote   = (int) $row['remote'];

        if ( $location !== '' ) {
            $country = roc_extract_country( $location );
            if ( $country !== null ) {
                $counts[ $country ] = ( $counts[ $country ] ?? 0 ) + 1;
                continue;
            }
        }

        if ( $remote ) {
            $counts['Remote'] = ( $counts['Remote'] ?? 0 ) + 1;
        }
    }

    $data = [];
    foreach ( $counts as $country => $count ) {
        $iso  = ( $country === 'Remote' ) ? '' : roc_country_iso( $country );
        $flag = ( $country === 'Remote' ) ? '🌐' : roc_flag_emoji( $iso );
        $slug = roc_country_slug( $country );
        $url  = ( $country === 'Remote' ) ? '/remote-jobs/' : roc_country_page_url( $country );
        $data[] = [
            'country' => $country,
            'iso'     => $iso,
            'flag'    => $flag,
            'count'   => $count,
            'slug'    => $slug,
            'url'     => $url,
        ];
    }

    // Sort: by count desc, then name asc; Remote always last
    usort( $data, function ( $a, $b ) {
        if ( $a['country'] === 'Remote' ) return 1;
        if ( $b['country'] === 'Remote' ) return -1;
        if ( $b['count'] !== $a['count'] ) return $b['count'] - $a['count'];
        return strcmp( $a['country'], $b['country'] );
    } );

    set_transient( 'roc_countries_v1', $data, HOUR_IN_SECONDS );

    return rest_ensure_response( $data );
}

// ---------------------------------------------------------------------------
// 4. Helpers
// ---------------------------------------------------------------------------

/**
 * Convert a country name to a URL-safe slug (e.g. "United States" → "united-states").
 */
function roc_country_slug( $country ) {
    $slug = strtolower( $country );
    $slug = preg_replace( '/[^a-z0-9]+/', '-', $slug );
    return trim( $slug, '-' );
}

/**
 * Extract the country from a location string such as "City, State, Country",
 * "DEU - Berlin", "Remote (DEU); Amsterdam (NLD)" or "Boston or Remote".
 * Returns the canonical country name, or null.
 */
function roc_extract_country( $location ) {
    foreach ( preg_split( '/\s*;\s*/', trim( $location ) ) as $segment ) {
        $parts = preg_split( '#\s*(?:,|\s-\s|-\s|/|\||\(|\)|\bor\b|_)\s*#iu', $segment, -1, PREG_SPLIT_NO_EMPTY );
        foreach ( array_reverse( $parts ) as $part ) {
            $country = roc_normalize_country( $part );
            if ( null !== $country ) {
                return $country;
            }
        }
    }
    return null;
}

/**
 * Map one location part (name, alias, ISO 3166 alpha-3 code, US state) to a
 * canonical country name, or null. Bare alpha-2 codes are only accepted when
 * the whole part is the code and it is not a US state ("SE" yes, "CA" = US).
 */
function roc_normalize_country( $raw ) {
    static $lookup = null, $iso2 = null;
    $key = strtolower( trim( $raw, " \t\n\r\0\x0B.*" ) );
    if ( '' === $key ) {
        return null;
    }
    if ( null === $lookup ) {
        $lookup = [];
        $iso2   = [];
        foreach ( roc_country_table() as $name => [ $a2, $a3, $alts ] ) {
            $lookup[ strtolower( $name ) ] = $name;
            $lookup[ strtolower( $a3 ) ]   = $name;
            foreach ( $alts as $alt ) {
                $lookup[ strtolower( $alt ) ] = $name;
            }
            $iso2[ strtolower( $a2 ) ] = $name;
        }
        $aliases = [
            'deutschland' => 'Germany', 'allemagne' => 'Germany',
            'united states of america' => 'United States', 'usa' => 'United States',
            'u.s.a' => 'United States', 'u.s' => 'United States', 'us' => 'United States',
            'england' => 'United Kingdom', 'scotland' => 'United Kingdom', 'wales' => 'United Kingdom',
            'northern ireland' => 'United Kingdom', 'great britain' => 'United Kingdom',
            'uk' => 'United Kingdom', 'u.k' => 'United Kingdom',
            'éire' => 'Ireland', 'republic of ireland' => 'Ireland',
            'korea' => 'South Korea', 'republic of korea' => 'South Korea',
            'uae' => 'United Arab Emirates', 'hong kong sar' => 'Hong Kong', 'hong kong s.a.r' => 'Hong Kong',
            'méxico' => 'Mexico', 'brasil' => 'Brazil', 'italia' => 'Italy', 'españa' => 'Spain',
            'nederland' => 'Netherlands', 'the netherlands' => 'Netherlands', 'holland' => 'Netherlands',
            'österreich' => 'Austria', 'schweiz' => 'Switzerland', 'suisse' => 'Switzerland',
            'svizzera' => 'Switzerland', 'polska' => 'Poland', 'czechia' => 'Czech Republic',
            'türkiye' => 'Turkey', 'turkiye' => 'Turkey', 'ivory coast' => "Côte d'Ivoire",
            'cote d\'ivoire' => "Côte d'Ivoire", 'palestine' => 'Palestinian Territories',
            'congo-brazzaville' => 'Congo-Brazzaville', 'republic of the congo' => 'Congo-Brazzaville',
            'democratic republic of the congo' => 'DR Congo', 'macau' => 'Macao',
            'swaziland' => 'Eswatini', 'burma' => 'Myanmar', 'east timor' => 'Timor-Leste',
            'kosova' => 'Kosovo', 'deutschland gmbh' => 'Germany',
            'san francisco bay area' => 'United States', 'new york metropolitan area' => 'United States',
            'greater toronto area' => 'Canada', 'greater london' => 'United Kingdom',
            'greater london area' => 'United Kingdom',
        ];
        $us_states = [ 'alabama','alaska','arizona','arkansas','california','colorado','connecticut','delaware',
            'district of columbia','florida','hawaii','idaho','illinois','indiana','iowa','kansas','kentucky',
            'louisiana','maine','maryland','massachusetts','michigan','minnesota','mississippi','missouri',
            'montana','nebraska','nevada','new hampshire','new jersey','new mexico','new york','north carolina',
            'north dakota','ohio','oklahoma','oregon','pennsylvania','rhode island','south carolina',
            'south dakota','tennessee','texas','utah','vermont','virginia','washington','west virginia',
            'wisconsin','wyoming','puerto rico','al','ak','az','ar','ca','co','ct','de','dc','fl','ga','hi',
            'id','il','in','ia','ks','ky','la','me','md','ma','mi','mn','ms','mo','mt','ne','nv','nh','nj',
            'nm','ny','nc','nd','oh','ok','or','pa','ri','sc','sd','tn','tx','ut','vt','va','wa','wv','wi',
            'wy','pr' ];
        foreach ( $us_states as $s ) {
            // Country names win over states ("Georgia" stays the country);
            // two-letter state codes win over ISO alpha-2 codes.
            $lookup[ $s ] = $lookup[ $s ] ?? 'United States';
            unset( $iso2[ $s ] );
        }
        $lookup = array_merge( $lookup, $aliases );
    }
    if ( isset( $lookup[ $key ] ) ) {
        return $lookup[ $key ];
    }
    if ( 2 === strlen( $key ) && isset( $iso2[ $key ] ) && strtoupper( trim( $raw ) ) === trim( $raw ) ) {
        return $iso2[ $key ];
    }
    return null;
}

function roc_country_iso( $country ) {
    return roc_country_table()[ $country ][0] ?? '';
}

/**
 * URL of the /jobs/country/{slug}/ page for a country. Creates the page
 * (same content as create_country_pages.py) the first time a new country
 * appears, so directory links never 404.
 */
function roc_country_page_url( $country ) {
    $slug = roc_country_slug( $country );
    $page = get_page_by_path( 'jobs/country/' . $slug );
    if ( ! $page ) {
        $parent = get_page_by_path( 'jobs/country' );
        // add_option() is atomic (unique option_name), so concurrent builds can't both create the page
        if ( ! $parent || ! add_option( 'roc_country_page_lock_' . $slug, time(), '', 'no' ) ) {
            return '/jobs/?search_location=' . rawurlencode( $country );
        }
        wp_insert_post( [
            'post_type'    => 'page',
            'post_status'  => 'publish',
            'post_parent'  => $parent->ID,
            'post_title'   => $country . ' Jobs',
            'post_name'    => $slug,
            'post_content' => "<!-- wp:paragraph -->\n<p>Browse open Revenue Operations, Sales Operations, and Marketing Operations jobs in "
                . esc_html( $country ) . ". Use the filters below to narrow your search.</p>\n<!-- /wp:paragraph -->\n\n"
                . "<!-- wp:shortcode -->\n[jobs location=\"" . esc_attr( $country ) . "\" show_filters=\"true\"]\n<!-- /wp:shortcode -->",
        ] );
        delete_option( 'roc_country_page_lock_' . $slug );
    }
    return '/jobs/country/' . $slug . '/';
}

// 301 old/unknown /jobs/country/{slug}/ URLs (e.g. "deu-berlin", "can-remote",
// "emea-remote") to the matching country page, /remote-jobs/ or /countries/.
add_action( 'template_redirect', function () {
    if ( ! is_404() ) return;
    $path = (string) wp_parse_url( $_SERVER['REQUEST_URI'] ?? '', PHP_URL_PATH );
    if ( ! preg_match( '#^/jobs/country/([^/]+)/?$#', $path, $m ) ) return;

    $tokens = explode( '-', strtolower( $m[1] ) );
    $target = '/countries/';
    $by_slug = [];
    foreach ( array_keys( roc_country_table() ) as $name ) {
        $by_slug[ roc_country_slug( $name ) ] = $name;
    }
    $country = roc_normalize_country( $tokens[0] ); // "deu-…", "can-remote"
    if ( null === $country ) {
        // Longest country slug found as a run of tokens ("remote-emea-united-kingdom")
        for ( $len = min( 4, count( $tokens ) ); $len >= 1 && null === $country; $len-- ) {
            for ( $i = 0; $i + $len <= count( $tokens ); $i++ ) {
                $run = implode( '-', array_slice( $tokens, $i, $len ) );
                if ( isset( $by_slug[ $run ] ) ) {
                    $country = $by_slug[ $run ];
                    break;
                }
            }
        }
    }
    if ( null !== $country && get_page_by_path( 'jobs/country/' . roc_country_slug( $country ) ) ) {
        $target = '/jobs/country/' . roc_country_slug( $country ) . '/';
    } elseif ( array_intersect( $tokens, [ 'remote', 'emea', 'apac', 'amer', 'americas', 'europe', 'europa', 'worldwide', 'global', 'anywhere' ] ) ) {
        $target = '/remote-jobs/';
    }
    wp_safe_redirect( home_url( $target ), 301, 'RevOpsCareers' );
    exit;
}, 2 );

/** ISO 3166-1 countries: name => [ alpha-2, alpha-3, alternative names ]. */
function roc_country_table() {
    static $countries = [
        'Afghanistan' => [ 'AF', 'AFG', [ 'Islamic Republic of Afghanistan' ] ],
        'Albania' => [ 'AL', 'ALB', [ 'Republic of Albania' ] ],
        'Algeria' => [ 'DZ', 'DZA', [ 'People\'s Democratic Republic of Algeria' ] ],
        'American Samoa' => [ 'AS', 'ASM', [  ] ],
        'Andorra' => [ 'AD', 'AND', [ 'Principality of Andorra' ] ],
        'Angola' => [ 'AO', 'AGO', [ 'Republic of Angola' ] ],
        'Anguilla' => [ 'AI', 'AIA', [  ] ],
        'Antarctica' => [ 'AQ', 'ATA', [  ] ],
        'Antigua and Barbuda' => [ 'AG', 'ATG', [  ] ],
        'Argentina' => [ 'AR', 'ARG', [ 'Argentine Republic' ] ],
        'Armenia' => [ 'AM', 'ARM', [ 'Republic of Armenia' ] ],
        'Aruba' => [ 'AW', 'ABW', [  ] ],
        'Australia' => [ 'AU', 'AUS', [  ] ],
        'Austria' => [ 'AT', 'AUT', [ 'Republic of Austria' ] ],
        'Azerbaijan' => [ 'AZ', 'AZE', [ 'Republic of Azerbaijan' ] ],
        'Bahamas' => [ 'BS', 'BHS', [ 'Commonwealth of the Bahamas' ] ],
        'Bahrain' => [ 'BH', 'BHR', [ 'Kingdom of Bahrain' ] ],
        'Bangladesh' => [ 'BD', 'BGD', [ 'People\'s Republic of Bangladesh' ] ],
        'Barbados' => [ 'BB', 'BRB', [  ] ],
        'Belarus' => [ 'BY', 'BLR', [ 'Republic of Belarus' ] ],
        'Belgium' => [ 'BE', 'BEL', [ 'Kingdom of Belgium' ] ],
        'Belize' => [ 'BZ', 'BLZ', [  ] ],
        'Benin' => [ 'BJ', 'BEN', [ 'Republic of Benin' ] ],
        'Bermuda' => [ 'BM', 'BMU', [  ] ],
        'Bhutan' => [ 'BT', 'BTN', [ 'Kingdom of Bhutan' ] ],
        'Bolivia' => [ 'BO', 'BOL', [ 'Bolivia, Plurinational State of', 'Plurinational State of Bolivia' ] ],
        'Bosnia and Herzegovina' => [ 'BA', 'BIH', [ 'Republic of Bosnia and Herzegovina' ] ],
        'Botswana' => [ 'BW', 'BWA', [ 'Republic of Botswana' ] ],
        'Bouvet Island' => [ 'BV', 'BVT', [  ] ],
        'Brazil' => [ 'BR', 'BRA', [ 'Federative Republic of Brazil' ] ],
        'British Indian Ocean Territory' => [ 'IO', 'IOT', [  ] ],
        'British Virgin Islands' => [ 'VG', 'VGB', [ 'Virgin Islands, British' ] ],
        'Brunei' => [ 'BN', 'BRN', [ 'Brunei Darussalam' ] ],
        'Bulgaria' => [ 'BG', 'BGR', [ 'Republic of Bulgaria' ] ],
        'Burkina Faso' => [ 'BF', 'BFA', [  ] ],
        'Burundi' => [ 'BI', 'BDI', [ 'Republic of Burundi' ] ],
        'Cambodia' => [ 'KH', 'KHM', [ 'Kingdom of Cambodia' ] ],
        'Cameroon' => [ 'CM', 'CMR', [ 'Republic of Cameroon' ] ],
        'Canada' => [ 'CA', 'CAN', [  ] ],
        'Cape Verde' => [ 'CV', 'CPV', [ 'Cabo Verde', 'Republic of Cabo Verde' ] ],
        'Caribbean Netherlands' => [ 'BQ', 'BES', [ 'Bonaire, Sint Eustatius and Saba' ] ],
        'Cayman Islands' => [ 'KY', 'CYM', [  ] ],
        'Central African Republic' => [ 'CF', 'CAF', [  ] ],
        'Chad' => [ 'TD', 'TCD', [ 'Republic of Chad' ] ],
        'Chile' => [ 'CL', 'CHL', [ 'Republic of Chile' ] ],
        'China' => [ 'CN', 'CHN', [ 'People\'s Republic of China' ] ],
        'Christmas Island' => [ 'CX', 'CXR', [  ] ],
        'Cocos (Keeling) Islands' => [ 'CC', 'CCK', [  ] ],
        'Colombia' => [ 'CO', 'COL', [ 'Republic of Colombia' ] ],
        'Comoros' => [ 'KM', 'COM', [ 'Union of the Comoros' ] ],
        'Congo-Brazzaville' => [ 'CG', 'COG', [ 'Congo', 'Republic of the Congo' ] ],
        'Cook Islands' => [ 'CK', 'COK', [  ] ],
        'Costa Rica' => [ 'CR', 'CRI', [ 'Republic of Costa Rica' ] ],
        'Croatia' => [ 'HR', 'HRV', [ 'Republic of Croatia' ] ],
        'Cuba' => [ 'CU', 'CUB', [ 'Republic of Cuba' ] ],
        'Curaçao' => [ 'CW', 'CUW', [  ] ],
        'Cyprus' => [ 'CY', 'CYP', [ 'Republic of Cyprus' ] ],
        'Czech Republic' => [ 'CZ', 'CZE', [ 'Czechia' ] ],
        'Côte d\'Ivoire' => [ 'CI', 'CIV', [ 'Republic of Côte d\'Ivoire' ] ],
        'DR Congo' => [ 'CD', 'COD', [ 'Congo, The Democratic Republic of the' ] ],
        'Denmark' => [ 'DK', 'DNK', [ 'Kingdom of Denmark' ] ],
        'Djibouti' => [ 'DJ', 'DJI', [ 'Republic of Djibouti' ] ],
        'Dominica' => [ 'DM', 'DMA', [ 'Commonwealth of Dominica' ] ],
        'Dominican Republic' => [ 'DO', 'DOM', [  ] ],
        'Ecuador' => [ 'EC', 'ECU', [ 'Republic of Ecuador' ] ],
        'Egypt' => [ 'EG', 'EGY', [ 'Arab Republic of Egypt' ] ],
        'El Salvador' => [ 'SV', 'SLV', [ 'Republic of El Salvador' ] ],
        'Equatorial Guinea' => [ 'GQ', 'GNQ', [ 'Republic of Equatorial Guinea' ] ],
        'Eritrea' => [ 'ER', 'ERI', [ 'the State of Eritrea' ] ],
        'Estonia' => [ 'EE', 'EST', [ 'Republic of Estonia' ] ],
        'Eswatini' => [ 'SZ', 'SWZ', [ 'Kingdom of Eswatini' ] ],
        'Ethiopia' => [ 'ET', 'ETH', [ 'Federal Democratic Republic of Ethiopia' ] ],
        'Falkland Islands' => [ 'FK', 'FLK', [ 'Falkland Islands (Malvinas)' ] ],
        'Faroe Islands' => [ 'FO', 'FRO', [  ] ],
        'Fiji' => [ 'FJ', 'FJI', [ 'Republic of Fiji' ] ],
        'Finland' => [ 'FI', 'FIN', [ 'Republic of Finland' ] ],
        'France' => [ 'FR', 'FRA', [ 'French Republic' ] ],
        'French Guiana' => [ 'GF', 'GUF', [  ] ],
        'French Polynesia' => [ 'PF', 'PYF', [  ] ],
        'French Southern Territories' => [ 'TF', 'ATF', [  ] ],
        'Gabon' => [ 'GA', 'GAB', [ 'Gabonese Republic' ] ],
        'Gambia' => [ 'GM', 'GMB', [ 'Republic of the Gambia' ] ],
        'Georgia' => [ 'GE', 'GEO', [  ] ],
        'Germany' => [ 'DE', 'DEU', [ 'Federal Republic of Germany' ] ],
        'Ghana' => [ 'GH', 'GHA', [ 'Republic of Ghana' ] ],
        'Gibraltar' => [ 'GI', 'GIB', [  ] ],
        'Greece' => [ 'GR', 'GRC', [ 'Hellenic Republic' ] ],
        'Greenland' => [ 'GL', 'GRL', [  ] ],
        'Grenada' => [ 'GD', 'GRD', [  ] ],
        'Guadeloupe' => [ 'GP', 'GLP', [  ] ],
        'Guam' => [ 'GU', 'GUM', [  ] ],
        'Guatemala' => [ 'GT', 'GTM', [ 'Republic of Guatemala' ] ],
        'Guernsey' => [ 'GG', 'GGY', [  ] ],
        'Guinea' => [ 'GN', 'GIN', [ 'Republic of Guinea' ] ],
        'Guinea-Bissau' => [ 'GW', 'GNB', [ 'Republic of Guinea-Bissau' ] ],
        'Guyana' => [ 'GY', 'GUY', [ 'Republic of Guyana' ] ],
        'Haiti' => [ 'HT', 'HTI', [ 'Republic of Haiti' ] ],
        'Heard Island and McDonald Islands' => [ 'HM', 'HMD', [  ] ],
        'Honduras' => [ 'HN', 'HND', [ 'Republic of Honduras' ] ],
        'Hong Kong' => [ 'HK', 'HKG', [ 'Hong Kong Special Administrative Region of China' ] ],
        'Hungary' => [ 'HU', 'HUN', [  ] ],
        'Iceland' => [ 'IS', 'ISL', [ 'Republic of Iceland' ] ],
        'India' => [ 'IN', 'IND', [ 'Republic of India' ] ],
        'Indonesia' => [ 'ID', 'IDN', [ 'Republic of Indonesia' ] ],
        'Iran' => [ 'IR', 'IRN', [ 'Iran, Islamic Republic of', 'Islamic Republic of Iran' ] ],
        'Iraq' => [ 'IQ', 'IRQ', [ 'Republic of Iraq' ] ],
        'Ireland' => [ 'IE', 'IRL', [  ] ],
        'Isle of Man' => [ 'IM', 'IMN', [  ] ],
        'Israel' => [ 'IL', 'ISR', [ 'State of Israel' ] ],
        'Italy' => [ 'IT', 'ITA', [ 'Italian Republic' ] ],
        'Jamaica' => [ 'JM', 'JAM', [  ] ],
        'Japan' => [ 'JP', 'JPN', [  ] ],
        'Jersey' => [ 'JE', 'JEY', [  ] ],
        'Jordan' => [ 'JO', 'JOR', [ 'Hashemite Kingdom of Jordan' ] ],
        'Kazakhstan' => [ 'KZ', 'KAZ', [ 'Republic of Kazakhstan' ] ],
        'Kenya' => [ 'KE', 'KEN', [ 'Republic of Kenya' ] ],
        'Kiribati' => [ 'KI', 'KIR', [ 'Republic of Kiribati' ] ],
        'Kosovo' => [ 'XK', 'XKX', [  ] ],
        'Kuwait' => [ 'KW', 'KWT', [ 'State of Kuwait' ] ],
        'Kyrgyzstan' => [ 'KG', 'KGZ', [ 'Kyrgyz Republic' ] ],
        'Laos' => [ 'LA', 'LAO', [ 'Lao People\'s Democratic Republic' ] ],
        'Latvia' => [ 'LV', 'LVA', [ 'Republic of Latvia' ] ],
        'Lebanon' => [ 'LB', 'LBN', [ 'Lebanese Republic' ] ],
        'Lesotho' => [ 'LS', 'LSO', [ 'Kingdom of Lesotho' ] ],
        'Liberia' => [ 'LR', 'LBR', [ 'Republic of Liberia' ] ],
        'Libya' => [ 'LY', 'LBY', [  ] ],
        'Liechtenstein' => [ 'LI', 'LIE', [ 'Principality of Liechtenstein' ] ],
        'Lithuania' => [ 'LT', 'LTU', [ 'Republic of Lithuania' ] ],
        'Luxembourg' => [ 'LU', 'LUX', [ 'Grand Duchy of Luxembourg' ] ],
        'Macao' => [ 'MO', 'MAC', [ 'Macao Special Administrative Region of China' ] ],
        'Madagascar' => [ 'MG', 'MDG', [ 'Republic of Madagascar' ] ],
        'Malawi' => [ 'MW', 'MWI', [ 'Republic of Malawi' ] ],
        'Malaysia' => [ 'MY', 'MYS', [  ] ],
        'Maldives' => [ 'MV', 'MDV', [ 'Republic of Maldives' ] ],
        'Mali' => [ 'ML', 'MLI', [ 'Republic of Mali' ] ],
        'Malta' => [ 'MT', 'MLT', [ 'Republic of Malta' ] ],
        'Marshall Islands' => [ 'MH', 'MHL', [ 'Republic of the Marshall Islands' ] ],
        'Martinique' => [ 'MQ', 'MTQ', [  ] ],
        'Mauritania' => [ 'MR', 'MRT', [ 'Islamic Republic of Mauritania' ] ],
        'Mauritius' => [ 'MU', 'MUS', [ 'Republic of Mauritius' ] ],
        'Mayotte' => [ 'YT', 'MYT', [  ] ],
        'Mexico' => [ 'MX', 'MEX', [ 'United Mexican States' ] ],
        'Micronesia' => [ 'FM', 'FSM', [ 'Federated States of Micronesia', 'Micronesia, Federated States of' ] ],
        'Moldova' => [ 'MD', 'MDA', [ 'Moldova, Republic of', 'Republic of Moldova' ] ],
        'Monaco' => [ 'MC', 'MCO', [ 'Principality of Monaco' ] ],
        'Mongolia' => [ 'MN', 'MNG', [  ] ],
        'Montenegro' => [ 'ME', 'MNE', [  ] ],
        'Montserrat' => [ 'MS', 'MSR', [  ] ],
        'Morocco' => [ 'MA', 'MAR', [ 'Kingdom of Morocco' ] ],
        'Mozambique' => [ 'MZ', 'MOZ', [ 'Republic of Mozambique' ] ],
        'Myanmar' => [ 'MM', 'MMR', [ 'Republic of Myanmar' ] ],
        'Namibia' => [ 'NA', 'NAM', [ 'Republic of Namibia' ] ],
        'Nauru' => [ 'NR', 'NRU', [ 'Republic of Nauru' ] ],
        'Nepal' => [ 'NP', 'NPL', [ 'Federal Democratic Republic of Nepal' ] ],
        'Netherlands' => [ 'NL', 'NLD', [ 'Kingdom of the Netherlands' ] ],
        'New Caledonia' => [ 'NC', 'NCL', [  ] ],
        'New Zealand' => [ 'NZ', 'NZL', [  ] ],
        'Nicaragua' => [ 'NI', 'NIC', [ 'Republic of Nicaragua' ] ],
        'Niger' => [ 'NE', 'NER', [ 'Republic of the Niger' ] ],
        'Nigeria' => [ 'NG', 'NGA', [ 'Federal Republic of Nigeria' ] ],
        'Niue' => [ 'NU', 'NIU', [  ] ],
        'Norfolk Island' => [ 'NF', 'NFK', [  ] ],
        'North Korea' => [ 'KP', 'PRK', [ 'Democratic People\'s Republic of Korea', 'Korea, Democratic People\'s Republic of' ] ],
        'North Macedonia' => [ 'MK', 'MKD', [ 'Republic of North Macedonia' ] ],
        'Northern Mariana Islands' => [ 'MP', 'MNP', [ 'Commonwealth of the Northern Mariana Islands' ] ],
        'Norway' => [ 'NO', 'NOR', [ 'Kingdom of Norway' ] ],
        'Oman' => [ 'OM', 'OMN', [ 'Sultanate of Oman' ] ],
        'Pakistan' => [ 'PK', 'PAK', [ 'Islamic Republic of Pakistan' ] ],
        'Palau' => [ 'PW', 'PLW', [ 'Republic of Palau' ] ],
        'Palestinian Territories' => [ 'PS', 'PSE', [ 'Palestine, State of', 'the State of Palestine' ] ],
        'Panama' => [ 'PA', 'PAN', [ 'Republic of Panama' ] ],
        'Papua New Guinea' => [ 'PG', 'PNG', [ 'Independent State of Papua New Guinea' ] ],
        'Paraguay' => [ 'PY', 'PRY', [ 'Republic of Paraguay' ] ],
        'Peru' => [ 'PE', 'PER', [ 'Republic of Peru' ] ],
        'Philippines' => [ 'PH', 'PHL', [ 'Republic of the Philippines' ] ],
        'Pitcairn Islands' => [ 'PN', 'PCN', [ 'Pitcairn' ] ],
        'Poland' => [ 'PL', 'POL', [ 'Republic of Poland' ] ],
        'Portugal' => [ 'PT', 'PRT', [ 'Portuguese Republic' ] ],
        'Puerto Rico' => [ 'PR', 'PRI', [  ] ],
        'Qatar' => [ 'QA', 'QAT', [ 'State of Qatar' ] ],
        'Romania' => [ 'RO', 'ROU', [  ] ],
        'Russia' => [ 'RU', 'RUS', [ 'Russian Federation' ] ],
        'Rwanda' => [ 'RW', 'RWA', [ 'Rwandese Republic' ] ],
        'Réunion' => [ 'RE', 'REU', [  ] ],
        'Saint Barthélemy' => [ 'BL', 'BLM', [  ] ],
        'Saint Helena' => [ 'SH', 'SHN', [ 'Saint Helena, Ascension and Tristan da Cunha' ] ],
        'Saint Kitts and Nevis' => [ 'KN', 'KNA', [  ] ],
        'Saint Lucia' => [ 'LC', 'LCA', [  ] ],
        'Saint Martin' => [ 'MF', 'MAF', [ 'Saint Martin (French part)' ] ],
        'Saint Pierre and Miquelon' => [ 'PM', 'SPM', [  ] ],
        'Saint Vincent and the Grenadines' => [ 'VC', 'VCT', [  ] ],
        'Samoa' => [ 'WS', 'WSM', [ 'Independent State of Samoa' ] ],
        'San Marino' => [ 'SM', 'SMR', [ 'Republic of San Marino' ] ],
        'Sao Tome and Principe' => [ 'ST', 'STP', [ 'Democratic Republic of Sao Tome and Principe' ] ],
        'Saudi Arabia' => [ 'SA', 'SAU', [ 'Kingdom of Saudi Arabia' ] ],
        'Senegal' => [ 'SN', 'SEN', [ 'Republic of Senegal' ] ],
        'Serbia' => [ 'RS', 'SRB', [ 'Republic of Serbia' ] ],
        'Seychelles' => [ 'SC', 'SYC', [ 'Republic of Seychelles' ] ],
        'Sierra Leone' => [ 'SL', 'SLE', [ 'Republic of Sierra Leone' ] ],
        'Singapore' => [ 'SG', 'SGP', [ 'Republic of Singapore' ] ],
        'Sint Maarten' => [ 'SX', 'SXM', [ 'Sint Maarten (Dutch part)' ] ],
        'Slovakia' => [ 'SK', 'SVK', [ 'Slovak Republic' ] ],
        'Slovenia' => [ 'SI', 'SVN', [ 'Republic of Slovenia' ] ],
        'Solomon Islands' => [ 'SB', 'SLB', [  ] ],
        'Somalia' => [ 'SO', 'SOM', [ 'Federal Republic of Somalia' ] ],
        'South Africa' => [ 'ZA', 'ZAF', [ 'Republic of South Africa' ] ],
        'South Georgia and the South Sandwich Islands' => [ 'GS', 'SGS', [  ] ],
        'South Korea' => [ 'KR', 'KOR', [ 'Korea, Republic of' ] ],
        'South Sudan' => [ 'SS', 'SSD', [ 'Republic of South Sudan' ] ],
        'Spain' => [ 'ES', 'ESP', [ 'Kingdom of Spain' ] ],
        'Sri Lanka' => [ 'LK', 'LKA', [ 'Democratic Socialist Republic of Sri Lanka' ] ],
        'Sudan' => [ 'SD', 'SDN', [ 'Republic of the Sudan' ] ],
        'Suriname' => [ 'SR', 'SUR', [ 'Republic of Suriname' ] ],
        'Svalbard and Jan Mayen' => [ 'SJ', 'SJM', [  ] ],
        'Sweden' => [ 'SE', 'SWE', [ 'Kingdom of Sweden' ] ],
        'Switzerland' => [ 'CH', 'CHE', [ 'Swiss Confederation' ] ],
        'Syria' => [ 'SY', 'SYR', [ 'Syrian Arab Republic' ] ],
        'Taiwan' => [ 'TW', 'TWN', [ 'Taiwan, Province of China' ] ],
        'Tajikistan' => [ 'TJ', 'TJK', [ 'Republic of Tajikistan' ] ],
        'Tanzania' => [ 'TZ', 'TZA', [ 'Tanzania, United Republic of', 'United Republic of Tanzania' ] ],
        'Thailand' => [ 'TH', 'THA', [ 'Kingdom of Thailand' ] ],
        'Timor-Leste' => [ 'TL', 'TLS', [ 'Democratic Republic of Timor-Leste' ] ],
        'Togo' => [ 'TG', 'TGO', [ 'Togolese Republic' ] ],
        'Tokelau' => [ 'TK', 'TKL', [  ] ],
        'Tonga' => [ 'TO', 'TON', [ 'Kingdom of Tonga' ] ],
        'Trinidad and Tobago' => [ 'TT', 'TTO', [ 'Republic of Trinidad and Tobago' ] ],
        'Tunisia' => [ 'TN', 'TUN', [ 'Republic of Tunisia' ] ],
        'Turkey' => [ 'TR', 'TUR', [ 'Republic of Türkiye', 'Türkiye' ] ],
        'Turkmenistan' => [ 'TM', 'TKM', [  ] ],
        'Turks and Caicos Islands' => [ 'TC', 'TCA', [  ] ],
        'Tuvalu' => [ 'TV', 'TUV', [  ] ],
        'U.S. Virgin Islands' => [ 'VI', 'VIR', [ 'Virgin Islands of the United States', 'Virgin Islands, U.S.' ] ],
        'Uganda' => [ 'UG', 'UGA', [ 'Republic of Uganda' ] ],
        'Ukraine' => [ 'UA', 'UKR', [  ] ],
        'United Arab Emirates' => [ 'AE', 'ARE', [  ] ],
        'United Kingdom' => [ 'GB', 'GBR', [ 'United Kingdom of Great Britain and Northern Ireland' ] ],
        'United States' => [ 'US', 'USA', [ 'United States of America' ] ],
        'United States Minor Outlying Islands' => [ 'UM', 'UMI', [  ] ],
        'Uruguay' => [ 'UY', 'URY', [ 'Eastern Republic of Uruguay' ] ],
        'Uzbekistan' => [ 'UZ', 'UZB', [ 'Republic of Uzbekistan' ] ],
        'Vanuatu' => [ 'VU', 'VUT', [ 'Republic of Vanuatu' ] ],
        'Vatican City' => [ 'VA', 'VAT', [ 'Holy See (Vatican City State)' ] ],
        'Venezuela' => [ 'VE', 'VEN', [ 'Bolivarian Republic of Venezuela', 'Venezuela, Bolivarian Republic of' ] ],
        'Vietnam' => [ 'VN', 'VNM', [ 'Socialist Republic of Viet Nam', 'Viet Nam' ] ],
        'Wallis and Futuna' => [ 'WF', 'WLF', [  ] ],
        'Western Sahara' => [ 'EH', 'ESH', [  ] ],
        'Yemen' => [ 'YE', 'YEM', [ 'Republic of Yemen' ] ],
        'Zambia' => [ 'ZM', 'ZMB', [ 'Republic of Zambia' ] ],
        'Zimbabwe' => [ 'ZW', 'ZWE', [ 'Republic of Zimbabwe' ] ],
        'Åland Islands' => [ 'AX', 'ALA', [  ] ],
    ];
    return $countries;
}

function roc_flag_emoji( $iso ) {
    if ( ! $iso || strlen( $iso ) !== 2 ) {
        return '🌍';
    }
    $base   = 0x1F1E6 - ord( 'A' );
    $chars  = str_split( strtoupper( $iso ) );
    $result = '';
    foreach ( $chars as $c ) {
        $result .= mb_chr( $base + ord( $c ), 'UTF-8' );
    }
    return $result;
}

/**
 * RevOpsCareers – Company pages at /jobs/company/{slug}/
 * + /wp-json/roc/v1/companies endpoint for the /companies/ directory
 *
 * Replaces the original "Dynamic company pages" block in the child theme
 * functions.php (2026-10-07). After deploying run: wp rewrite flush
 *
 * - Name variants ("Amazon", "Amazon Inc.", "Google LLC") are grouped by
 *   roc_company_key() into one company; the most-used variant names the page
 *   and other variant URLs 301 to it.
 * - Logo: first real logo among the variants' latest jobs; the fallback
 *   favicon (ID 184) and letter placeholders (option roc_placeholder_logo_ids)
 *   don't count. No real logo → ROC favicon.
 * - Pages render through the /jobs/company/ WP page (normal theme template)
 *   with the WP Job Manager [jobs] list restricted to the company.
 */

define( 'ROC_FALLBACK_LOGO_ID', 184 );
define( 'ROC_COMPANY_PAGE_ID', 34520 ); // /jobs/company/

// ---------------------------------------------------------------------------
// 1. Routing: /jobs/company/{slug}/ → the /jobs/company/ page with roc_company
// ---------------------------------------------------------------------------
add_filter( 'query_vars', function ( $vars ) {
    $vars[] = 'roc_company';
    return $vars;
} );

add_action( 'init', function () {
    add_rewrite_rule(
        '^jobs/company/([^/]+)/?$',
        'index.php?page_id=' . ROC_COMPANY_PAGE_ID . '&roc_company=$matches[1]',
        'top'
    );
} );

/** The company being viewed (set in template_redirect), or null. */
function roc_current_company() {
    return $GLOBALS['roc_current_company'] ?? null;
}

add_action( 'template_redirect', function () {
    if ( ! is_page( ROC_COMPANY_PAGE_ID ) ) return;

    $slug = get_query_var( 'roc_company' );
    if ( ! $slug ) {
        // The bare /jobs/company/ page is superseded by /companies/
        wp_safe_redirect( home_url( '/companies/' ), 301, 'RevOpsCareers' );
        exit;
    }

    $company = roc_company_find_by_slug( $slug );
    if ( ! $company ) {
        global $wp_query;
        $wp_query->set_404();
        status_header( 404 );
        nocache_headers();
        // Render now: returning would let redirect_canonical() 301 to /jobs/company/
        include get_query_template( '404' );
        exit;
    }
    if ( $company['slug'] !== $slug ) {
        wp_safe_redirect( home_url( $company['url'] ), 301, 'RevOpsCareers' );
        exit;
    }
    $GLOBALS['roc_current_company'] = $company;
}, 5 );

// Titles, canonical and description for company pages
add_filter( 'the_title', function ( $title, $id = 0 ) {
    $c = roc_current_company();
    return ( $c && (int) $id === ROC_COMPANY_PAGE_ID ) ? 'Jobs at ' . $c['company'] : $title;
}, 10, 2 );

foreach ( [ 'the_seo_framework_title_from_custom_field', 'the_seo_framework_title_from_generation' ] as $roc_hook ) {
    add_filter( $roc_hook, function ( $title ) {
        $c = roc_current_company();
        return $c ? 'Revenue Operations Jobs at ' . $c['company'] : $title;
    } );
}

add_filter( 'the_seo_framework_meta_render_data', function ( $tags ) {
    $c = roc_current_company();
    if ( ! $c ) return $tags;
    $url  = home_url( $c['url'] );
    $desc = $c['description'] ?: sprintf( 'Browse %d open RevOps, GTM, sales and marketing jobs at %s on RevOps Careers.', $c['count'], $c['company'] );
    foreach ( [ 'canonical' => 'href', 'og:url' => 'content' ] as $key => $attr ) {
        if ( isset( $tags[ $key ]['attributes'] ) ) $tags[ $key ]['attributes'][ $attr ] = $url;
    }
    foreach ( [ 'description', 'og:description', 'twitter:description' ] as $key ) {
        if ( isset( $tags[ $key ]['attributes'] ) ) $tags[ $key ]['attributes']['content'] = wp_strip_all_tags( $desc );
    }
    return $tags;
} );

// Page content: company card + [jobs] list
add_filter( 'the_content', function ( $content ) {
    $c = roc_current_company();
    if ( ! $c || ! in_the_loop() || get_the_ID() !== ROC_COMPANY_PAGE_ID ) return $content;

    ob_start();
    ?>
<style>
#roc-co-header{display:flex;align-items:flex-start;gap:24px;margin-bottom:28px;padding:24px;background:#fff;border:1px solid #e5e7eb;border-radius:12px}
#roc-co-logo{width:80px;height:80px;object-fit:contain;border-radius:8px;flex-shrink:0;background:#f9fafb;border:1px solid #f0f0f0}
#roc-co-info{flex:1;min-width:0}
#roc-co-name{font-size:22px;font-weight:700;color:#111827;margin:0 0 6px}
#roc-co-count{font-size:14px;color:#6b7280;margin:0 0 10px}
#roc-co-desc{font-size:15px;color:#4b5563;line-height:1.6;margin:0 0 12px}
.roc-co-links{display:flex;gap:10px;flex-wrap:wrap}
.roc-co-link{font-size:13px;padding:6px 14px;border-radius:6px;text-decoration:none;font-weight:500;border:1px solid #e5e7eb;color:#374151;background:#fff}
.roc-co-link:hover{background:#f9fafb;border-color:#d1d5db}
@media (max-width:575px){#roc-co-header{flex-direction:column;gap:14px}}
</style>
<p><a href="/companies/">← All companies</a></p>
<div id="roc-co-header">
  <img id="roc-co-logo" src="<?php echo esc_url( $c['logo_url'] ); ?>" alt="<?php echo esc_attr( $c['company'] ); ?> logo">
  <div id="roc-co-info">
    <h2 id="roc-co-name"><?php echo esc_html( $c['company'] ); ?></h2>
    <p id="roc-co-count"><?php echo esc_html( number_format_i18n( $c['count'] ) . ' open position' . ( 1 === $c['count'] ? '' : 's' ) ); ?></p>
<?php if ( $c['description'] ) : ?>
    <p id="roc-co-desc"><?php echo esc_html( $c['description'] ); ?></p>
<?php endif; ?>
    <div class="roc-co-links">
<?php if ( $c['website'] ) : ?>
      <a class="roc-co-link" href="<?php echo esc_url( $c['website'] ); ?>" target="_blank" rel="noopener nofollow">🌐 Website</a>
<?php endif; ?>
<?php if ( $c['linkedin'] ) : ?>
      <a class="roc-co-link" href="<?php echo esc_url( $c['linkedin'] ); ?>" target="_blank" rel="noopener nofollow">💼 LinkedIn</a>
<?php endif; ?>
    </div>
  </div>
</div>
<?php
    return ob_get_clean() . do_shortcode( '[jobs show_filters="true" per_page="20"]' );
}, 20 );

// All company pages are the same WP page (34520), and WPJM keys its sessionStorage
// results cache by post ID — so it would restore the previous company's jobs.
add_filter( 'body_class', function ( $classes ) {
    if ( roc_current_company() ) {
        $classes[] = 'disable-job-manager-form-state-storage';
    }
    return $classes;
} );

// Restrict the [jobs] list (and its AJAX search) to the company's name variants
add_action( 'job_manager_job_filters_search_jobs_end', function () {
    $c = roc_current_company();
    if ( $c ) {
        echo '<input type="hidden" name="roc_company" value="' . esc_attr( $c['slug'] ) . '">';
    }
} );

add_filter( 'get_job_listings_query_args', function ( $query_args ) {
    if ( ! wp_doing_ajax() || empty( $_REQUEST['form_data'] ) ) return $query_args;
    parse_str( wp_unslash( $_REQUEST['form_data'] ), $form );
    if ( empty( $form['roc_company'] ) ) return $query_args;

    $c = roc_company_find_by_slug( sanitize_title( $form['roc_company'] ) );
    $query_args['meta_query']   = $query_args['meta_query'] ?? [];
    $query_args['meta_query'][] = [
        'key'     => '_company_name',
        'value'   => $c ? $c['names'] : [ '' ],
        'compare' => 'IN',
    ];
    // WPJM caches listing queries by args; vary the cache key by company
    $query_args['roc_company'] = $form['roc_company'];
    return $query_args;
} );

// ---------------------------------------------------------------------------
// 2. Lookup
// ---------------------------------------------------------------------------
function roc_company_find_by_slug( $slug ) {
    foreach ( [ roc_companies_page_data(), null ] as $data ) {
        if ( null === $data ) {
            // Not in the hourly copy (e.g. a company new since the last build)
            $data = rest_ensure_response( roc_companies_endpoint() )->get_data();
        }
        foreach ( $data as $c ) {
            if ( $c['slug'] === $slug || in_array( $slug, $c['slugs'] ?? [], true ) ) {
                return $c;
            }
        }
    }
    return null;
}

// ---------------------------------------------------------------------------
// 3. REST endpoint  GET /wp-json/roc/v1/companies
// ---------------------------------------------------------------------------
add_action( 'rest_api_init', function () {
    register_rest_route( 'roc/v1', '/companies', [
        'methods'             => 'GET',
        'callback'            => 'roc_companies_endpoint',
        'permission_callback' => '__return_true',
    ] );
} );

// Letter-placeholder logo IDs, so the sync scripts never reuse them
add_action( 'rest_api_init', function () {
    register_rest_route( 'roc/v1', '/placeholder-logos', [
        'methods'             => 'GET',
        'callback'            => function () {
            return rest_ensure_response( array_map( 'intval', (array) get_option( 'roc_placeholder_logo_ids', [] ) ) );
        },
        'permission_callback' => '__return_true',
    ] );
} );

add_action( 'save_post_job_listing', 'roc_companies_bust_cache' );
add_action( 'trash_post',            'roc_companies_bust_cache' );
add_action( 'delete_post',           'roc_companies_bust_cache' );

function roc_companies_bust_cache() {
    delete_transient( 'roc_companies_v2' );
}

function roc_companies_endpoint() {
    $cached = get_transient( 'roc_companies_v2' );
    if ( false !== $cached ) {
        return rest_ensure_response( $cached );
    }

    global $wpdb;

    // Step 1: company name → count + latest job ID
    $rows = $wpdb->get_results( "
        SELECT
            pm.meta_value  AS company_name,
            COUNT(*)       AS job_count,
            MAX(p.ID)      AS latest_job_id
        FROM {$wpdb->posts} p
        JOIN {$wpdb->postmeta} pm
            ON pm.post_id = p.ID AND pm.meta_key = '_company_name'
        WHERE p.post_type   = 'job_listing'
          AND p.post_status = 'publish'
          AND pm.meta_value != ''
        GROUP BY pm.meta_value
        ORDER BY job_count DESC, pm.meta_value ASC
    ", ARRAY_A );

    if ( ! $rows ) return rest_ensure_response( [] );

    // Step 2: batch-load meta for all latest job IDs
    update_meta_cache( 'post', array_column( $rows, 'latest_job_id' ) );

    $placeholders = array_map( 'intval', (array) get_option( 'roc_placeholder_logo_ids', [] ) );
    $placeholders[] = ROC_FALLBACK_LOGO_ID;
    $fallback_url   = wp_get_attachment_image_url( ROC_FALLBACK_LOGO_ID, 'thumbnail' );

    // Step 3: group name variants; rows are sorted by count, so the first
    // variant of each group is the most-used one
    $groups = [];
    foreach ( $rows as $row ) {
        $key = roc_company_key( $row['company_name'] );
        if ( '' === $key ) continue;
        $job_id = (int) $row['latest_job_id'];

        if ( ! isset( $groups[ $key ] ) ) {
            $groups[ $key ] = [
                'company'     => $row['company_name'],
                'slug'        => roc_company_slug( $row['company_name'] ),
                'count'       => 0,
                'logo_url'    => '',
                'website'     => '',
                'linkedin'    => '',
                'description' => '',
                'names'       => [],
                'slugs'       => [],
            ];
        }
        $g = &$groups[ $key ];
        $g['count']  += (int) $row['job_count'];
        $g['names'][] = $row['company_name'];
        $g['slugs'][] = roc_company_slug( $row['company_name'] );

        $thumb_id = (int) get_post_thumbnail_id( $job_id );
        if ( ! $g['logo_url'] && $thumb_id && ! in_array( $thumb_id, $placeholders, true ) ) {
            $g['logo_url'] = (string) wp_get_attachment_image_url( $thumb_id, 'thumbnail' );
        }
        if ( ! $g['website'] ) {
            $g['website'] = (string) get_post_meta( $job_id, '_company_website', true );
        }
        if ( ! $g['linkedin'] ) {
            $g['linkedin'] = (string) get_post_meta( $job_id, '_company_linkedin', true );
        }
        if ( ! $g['description'] ) {
            $g['description'] = (string) ( get_post_meta( $job_id, '_company_brandfetch_description', true )
                ?: get_post_meta( $job_id, '_company_tagline', true ) );
        }
        unset( $g );
    }

    $data = [];
    foreach ( $groups as $g ) {
        $g['logo_url'] = $g['logo_url'] ?: $fallback_url;
        $g['url']      = '/jobs/company/' . $g['slug'] . '/';
        $g['slugs']    = array_values( array_unique( $g['slugs'] ) );
        $data[]        = $g;
    }
    usort( $data, function ( $a, $b ) {
        return $b['count'] <=> $a['count'] ?: strcasecmp( $a['company'], $b['company'] );
    } );

    set_transient( 'roc_companies_v2', $data, HOUR_IN_SECONDS );
    return rest_ensure_response( $data );
}

// ---------------------------------------------------------------------------
// 4. Helpers
// ---------------------------------------------------------------------------
function roc_company_slug( $name ) {
    $slug = strtolower( $name );
    $slug = preg_replace( '/[^a-z0-9]+/', '-', $slug );
    return trim( $slug, '-' );
}

/**
 * Grouping key for company name variants: lowercase, no legal suffix
 * (Inc, LLC, Ltd, GmbH, …) or ".com", letters and digits only.
 * "Amazon Inc." → "amazon", "Eleven Labs" → "elevenlabs".
 */
function roc_company_key( $name ) {
    $k = strtolower( trim( $name ) );
    $k = preg_replace( '/\.com$/', '', $k );
    $suffix = '/([\s,-]+(inc|incorporated|llc|l\.l\.c|ltd|limited|corp|corporation|co|gmbh|ag|plc|pty|bv|b\.v|sa|s\.a|srl|s\.r\.l|lp|llp|nv|oy|ab|as|spa|s\.p\.a|sas|kg|pte)\.?)+$/';
    for ( $i = 0; $i < 2; $i++ ) {
        $k = trim( preg_replace( $suffix, '', $k ), ' ,.-' );
    }
    return preg_replace( '/[^a-z0-9]+/', '', $k );
}

/**
 * Return a Brandfetch CDN logo URL for a given website URL, or '' if no domain found.
 */
function roc_brandfetch_logo_url( $website ) {
    if ( ! $website ) return '';
    $host = wp_parse_url( $website, PHP_URL_HOST );
    if ( ! $host ) return '';
    $domain = preg_replace( '/^www\./', '', strtolower( $host ) );
    if ( ! $domain ) return '';
    return 'https://cdn.brandfetch.io/domain/' . $domain . '/icon?c=1idzcKyyhEOReH2YGpD';
}

// ---------------------------------------------------------------------------
// Redirect jobs expired 60+ days ago to their job category (fallback: /jobs/)
// ---------------------------------------------------------------------------
add_action( 'template_redirect', function () {
    if ( ! is_singular( 'job_listing' ) || get_post_status() !== 'expired' ) return;

    $expires = get_post_meta( get_the_ID(), '_job_expires', true );
    if ( ! $expires || strtotime( $expires ) > strtotime( '-60 days' ) ) return;

    $target = home_url( '/jobs/' );
    $terms  = get_the_terms( get_the_ID(), 'job_listing_category' );
    if ( $terms && ! is_wp_error( $terms ) ) {
        $link = get_term_link( $terms[0] );
        if ( ! is_wp_error( $link ) ) $target = $link;
    }

    wp_safe_redirect( $target, 301, 'RevOpsCareers' );
    exit;
} );

// ---------------------------------------------------------------------------
// REST: dedup keys for the sync scripts (one query instead of ~230 REST pages)
// ---------------------------------------------------------------------------
function roc_job_dedup_rows() {
    global $wpdb;
    return $wpdb->get_results(
        "SELECT p.post_name AS slug, p.post_title AS title,
                MAX(CASE WHEN m.meta_key = '_application'  THEN m.meta_value END) AS application,
                MAX(CASE WHEN m.meta_key = '_company_name' THEN m.meta_value END) AS company,
                MAX(CASE WHEN m.meta_key = '_job_location' THEN m.meta_value END) AS location
         FROM {$wpdb->posts} p
         LEFT JOIN {$wpdb->postmeta} m
           ON m.post_id = p.ID AND m.meta_key IN ('_application', '_company_name', '_job_location')
         WHERE p.post_type = 'job_listing' AND p.post_status IN ('publish', 'draft')
         GROUP BY p.ID",
        ARRAY_A
    );
}

add_action( 'rest_api_init', function () {
    register_rest_route( 'roc/v1', '/job-dedup-keys', [
        'methods'             => 'GET',
        'callback'            => 'roc_job_dedup_rows',
        'permission_callback' => function () { return current_user_can( 'edit_posts' ); },
    ] );
} );

// ---------------------------------------------------------------------------
// /go/{job_id}/ – outbound apply redirect for every URL apply link
// (paid partners Lensa / WhatJobs, plus 'direct' employer links).
// Keeps apply URLs out of the page HTML, filters bots, logs every click.
// ---------------------------------------------------------------------------
const ROC_GO_PARTNERS = [ 'lensa.com' => 'lensa', 'whatjobs.com' => 'whatjobs' ];

function roc_go_partner( $url ) {
    $host = (string) wp_parse_url( $url, PHP_URL_HOST );
    foreach ( ROC_GO_PARTNERS as $domain => $source ) {
        if ( $host === $domain || str_ends_with( $host, '.' . $domain ) ) return $source; // incl. www., en-sg.
    }
    return '';
}

// Click-log source for an apply URL: the partner, 'direct' for any other web link, '' if not a URL
function roc_go_source( $url ) {
    $partner = roc_go_partner( $url );
    if ( $partner ) return $partner;
    return in_array( strtolower( (string) wp_parse_url( $url, PHP_URL_SCHEME ) ), [ 'http', 'https' ], true ) ? 'direct' : '';
}

// One-time token for the Continue form (30 min, deleted on use): a POST to /go/ without one is not a click
function roc_go_new_token( $job_id ) {
    $token = wp_generate_password( 20, false );
    set_transient( 'roc_go_t_' . $token, (int) $job_id, 30 * MINUTE_IN_SECONDS );
    return $token;
}

function roc_go_use_token( $job_id, $token ) {
    if ( ! preg_match( '/^[A-Za-z0-9]{20}$/', $token ) ) return false;
    $key = 'roc_go_t_' . $token;
    if ( (int) get_transient( $key ) !== (int) $job_id ) return false;
    delete_transient( $key );
    return true;
}

// Fixed per-job token on the job page's apply link (?t=). Job pages sit in WP Fastest Cache with no expiry,
// so it can't be one-time; it only proves the link came from our page, not from a forged referrer.
function roc_go_link_token( $job_id ) {
    return substr( hash_hmac( 'sha256', 'roc_go|' . (int) $job_id, wp_salt( 'nonce' ) ), 0, 16 );
}

add_action( 'init', function () {
    add_rewrite_rule( '^go/([0-9]+)/?$', 'index.php?roc_go=$matches[1]', 'top' );

    if ( get_option( 'roc_clicks_db_version' ) !== '2' ) {
        global $wpdb;
        require_once ABSPATH . 'wp-admin/includes/upgrade.php';
        dbDelta( "CREATE TABLE {$wpdb->prefix}roc_clicks (
            id bigint unsigned NOT NULL AUTO_INCREMENT,
            job_id bigint unsigned NOT NULL,
            source varchar(20) NOT NULL,
            is_bot tinyint(1) NOT NULL DEFAULT 0,
            country char(2) NOT NULL DEFAULT '',
            referer_host varchar(100) NOT NULL DEFAULT '',
            user_agent varchar(255) NOT NULL DEFAULT '',
            outcome varchar(20) NOT NULL DEFAULT '',
            created_at datetime NOT NULL,
            PRIMARY KEY  (id),
            KEY created_at (created_at),
            KEY job_id (job_id)
        ) {$wpdb->get_charset_collate()};" );
        // v1 rows predate the referrer check: derive their outcome from the old rules
        $wpdb->query( "UPDATE {$wpdb->prefix}roc_clicks c LEFT JOIN {$wpdb->posts} p ON p.ID = c.job_id
            SET c.outcome = CASE WHEN c.is_bot = 1 THEN 'bot'
                WHEN c.country = IF(p.post_name LIKE 'whatjobs-sg-%', 'SG', 'US') THEN 'redirected' ELSE 'region' END
            WHERE c.outcome = ''" );
        update_option( 'roc_clicks_db_version', '2', false );
    }
} );

add_filter( 'query_vars', function ( $vars ) {
    $vars[] = 'roc_go';
    return $vars;
} );

add_action( 'template_redirect', function () {
    $job_id = (int) get_query_var( 'roc_go' );
    if ( ! $job_id ) return;

    $url     = get_post_type( $job_id ) === 'job_listing' ? trim( (string) get_post_meta( $job_id, '_application', true ) ) : '';
    $partner = $url ? roc_go_source( $url ) : '';
    if ( ! $partner ) {
        global $wp_query;
        $wp_query->set_404();
        status_header( 404 );
        return;
    }

    $ua      = substr( (string) ( $_SERVER['HTTP_USER_AGENT'] ?? '' ), 0, 255 );
    $is_bot  = $ua === '' || preg_match( '/bot|crawl|spider|slurp|scrap|fetch|preview|facebookexternalhit|embedly|curl|wget|python|java\/|go-http|okhttp|axios|node-fetch|headless|phantom|httpclient|libwww|monitor|checker|validator/i', $ua );
    $referer = substr( (string) wp_parse_url( (string) ( $_SERVER['HTTP_REFERER'] ?? '' ), PHP_URL_HOST ), 0, 100 );
    $country = strtoupper( substr( (string) ( $_SERVER['HTTP_CF_IPCOUNTRY'] ?? '' ), 0, 2 ) );

    // Partners only accept clicks from their region (same rule as the WPCode "Country Limits" snippet)
    $slug    = get_post_field( 'post_name', $job_id );
    $allowed = preg_match( '/^whatjobs-sg-/i', $slug ) ? 'SG' : ( preg_match( '/^(whatjobs|lensa)-/i', $slug ) ? 'US' : '' );

    // Real clicks come from our own job pages (GET with our referrer AND the link token), or the confirm button
    // below (POST with its one-time token). 2026-11-04: bots were sending /go/ requests with a forged referrer.
    $own_site  = preg_replace( '/^www\./', '', (string) wp_parse_url( home_url(), PHP_URL_HOST ) );
    $is_post   = ( $_SERVER['REQUEST_METHOD'] ?? '' ) === 'POST';
    $own_ref   = preg_replace( '/^www\./', '', $referer ) === $own_site;
    $confirmed = $is_post
        ? roc_go_use_token( $job_id, (string) ( $_POST['roc_t'] ?? '' ) )
        : $own_ref && hash_equals( roc_go_link_token( $job_id ), (string) ( $_GET['t'] ?? '' ) );

    if ( $is_bot )                                 $outcome = 'bot';
    elseif ( $allowed && $country !== $allowed ) $outcome = 'region';
    elseif ( ! $confirmed )                       $outcome = ( $is_post || $own_ref ) ? 'bad_token' : 'no_referrer';
    else                                          $outcome = 'redirected';

    global $wpdb;
    $wpdb->insert( "{$wpdb->prefix}roc_clicks", [
        'job_id'       => $job_id,
        'source'       => $partner,
        'is_bot'       => $is_bot ? 1 : 0,
        'country'      => $country,
        'referer_host' => $referer,
        'user_agent'   => $ua,
        'outcome'      => $outcome,
        'created_at'   => current_time( 'mysql', true ),
    ] );

    if ( ! defined( 'DONOTCACHEPAGE' ) ) define( 'DONOTCACHEPAGE', true );
    nocache_headers();
    header( 'X-Robots-Tag: noindex, nofollow' );
    $back = '<a href="' . esc_url( get_permalink( $job_id ) ) . '">Back to the job</a>';

    if ( $outcome === 'region' ) {
        status_header( 403 );
        echo '<!doctype html><meta name="robots" content="noindex"><p>Applications for this role are open to '
            . ( $allowed === 'SG' ? 'Singapore' : 'US' ) . '-based candidates only. ' . $back . '</p>';
        exit;
    }

    if ( $outcome === 'bot' ) {
        status_header( 403 );
        echo '<!doctype html><meta name="robots" content="noindex"><p>This link is for job seekers. ' . $back . '</p>';
        exit;
    }

    if ( $outcome === 'no_referrer' || $outcome === 'bad_token' ) {
        status_header( 200 );
        echo '<!doctype html><meta name="robots" content="noindex"><meta name="viewport" content="width=device-width,initial-scale=1">'
            . '<title>Continue to application</title>'
            . '<div style="font-family:system-ui,sans-serif;max-width:480px;margin:15vh auto;padding:0 16px;text-align:center">'
            . '<p>You are leaving RevOps Careers to apply for <strong>' . esc_html( get_the_title( $job_id ) ) . '</strong>.</p>'
            . '<form method="post" action="' . esc_url( home_url( '/go/' . $job_id . '/' ) ) . '">'
            . '<input type="hidden" name="roc_t" value="' . esc_attr( roc_go_new_token( $job_id ) ) . '">'
            . '<button type="submit" style="padding:12px 24px;font-size:16px;background:#1a56db;color:#fff;border:0;border-radius:6px;cursor:pointer">Continue to application</button>'
            . '</form><p style="margin-top:24px">' . $back . '</p></div>';
        exit;
    }

    wp_redirect( esc_url_raw( $url ), 302, 'RevOpsCareers' );
    exit;
} );

// Point every URL apply link at /go/{id}/ (the real URL never appears in the HTML)
add_filter( 'the_job_application_method', function ( $method, $post ) {
    if ( $method && $method->type === 'url' && roc_go_source( $method->url ) ) {
        $method->display_host = preg_replace( '/^www\./', '', (string) wp_parse_url( $method->url, PHP_URL_HOST ) );
        $method->url          = add_query_arg( 't', roc_go_link_token( $post->ID ), home_url( '/go/' . $post->ID . '/' ) );
    }
    return $method;
}, 10, 2 );

// Hide application URLs from the public REST API (logged-in editors still see them)
add_filter( 'rest_prepare_job_listing', function ( $response ) {
    if ( ! current_user_can( 'edit_posts' ) ) {
        $data = $response->get_data();
        if ( isset( $data['meta']['_application'] ) ) {
            unset( $data['meta']['_application'] );
            $response->set_data( $data );
        }
    }
    return $response;
} );

// ---------------------------------------------------------------------------
// Tools → Apply Clicks: report on the /go/ click log (wp_roc_clicks)
// ---------------------------------------------------------------------------
add_action( 'admin_menu', function () {
    add_management_page( 'Apply Clicks', 'Apply Clicks', 'manage_options', 'roc-apply-clicks', 'roc_apply_clicks_page' );
} );

function roc_apply_clicks_page() {
    global $wpdb;
    $t    = $wpdb->prefix . 'roc_clicks';
    $days = (int) ( $_GET['days'] ?? 30 );
    if ( ! in_array( $days, [ 7, 30, 90 ], true ) ) $days = 30;
    $from = gmdate( 'Y-m-d H:i:s', time() - $days * DAY_IN_SECONDS );

    $daily = $wpdb->get_results( $wpdb->prepare(
        "SELECT DATE(created_at) d, source,
                SUM(outcome = 'redirected') eligible,
                SUM(outcome IN ('no_referrer', 'bad_token')) no_referrer,
                SUM(outcome = 'region') other_region,
                SUM(outcome = 'bot') bots
         FROM $t WHERE created_at >= %s GROUP BY d, source ORDER BY d DESC, source", $from ) );

    $countries = $wpdb->get_results( $wpdb->prepare(
        "SELECT country, COUNT(*) n FROM $t WHERE is_bot = 0 AND created_at >= %s
         GROUP BY country ORDER BY n DESC LIMIT 15", $from ) );

    $per_page = 50;
    $total    = (int) $wpdb->get_var( "SELECT COUNT(*) FROM $t" );
    $pages    = max( 1, (int) ceil( $total / $per_page ) );
    $paged    = min( $pages, max( 1, (int) ( $_GET['paged'] ?? 1 ) ) );
    $latest   = $wpdb->get_results( $wpdb->prepare(
        "SELECT c.*, p.post_title FROM $t c LEFT JOIN {$wpdb->posts} p ON p.ID = c.job_id
         ORDER BY c.id DESC LIMIT %d OFFSET %d", $per_page, ( $paged - 1 ) * $per_page ) );

    $totals = [ 'eligible' => 0, 'no_referrer' => 0, 'other_region' => 0, 'bots' => 0 ];
    foreach ( $daily as $r ) foreach ( $totals as $k => $v ) $totals[ $k ] += (int) $r->$k;

    echo '<div class="wrap"><h1>Apply Clicks</h1>';
    echo '<p>Clicks on apply links via <code>/go/</code>: Lensa / WhatJobs partner links, and <em>direct</em> links to employer sites (since 2026-10-25). <strong>Eligible</strong> = redirected to the partner or employer: human, from the partner\'s region (US, or SG for WhatJobs SG), coming from our own job page &mdash; what the partners should count. <strong>No referrer</strong> = shown a &ldquo;Continue&rdquo; button instead (likely scrapers). Times are UTC.</p>';
    echo '<p>';
    foreach ( [ 7, 30, 90 ] as $d ) {
        $url = esc_url( add_query_arg( [ 'page' => 'roc-apply-clicks', 'days' => $d ], admin_url( 'tools.php' ) ) );
        echo $d === $days ? "<strong>Last $d days</strong> &nbsp; " : "<a href=\"$url\">Last $d days</a> &nbsp; ";
    }
    echo '</p>';
    printf( '<p><strong>Totals:</strong> %d eligible &middot; %d no referrer &middot; %d other regions &middot; %d bots</p>',
        $totals['eligible'], $totals['no_referrer'], $totals['other_region'], $totals['bots'] );

    echo '<h2>Per day</h2><table class="widefat striped" style="max-width:720px"><thead><tr><th>Date</th><th>Partner</th><th>Eligible</th><th>No referrer</th><th>Other regions</th><th>Bots</th></tr></thead><tbody>';
    foreach ( $daily as $r ) {
        printf( '<tr><td>%s</td><td>%s</td><td><strong>%d</strong></td><td>%d</td><td>%d</td><td>%d</td></tr>',
            esc_html( $r->d ), esc_html( $r->source ), $r->eligible, $r->no_referrer, $r->other_region, $r->bots );
    }
    echo $daily ? '' : '<tr><td colspan="6">No clicks yet.</td></tr>';
    echo '</tbody></table>';

    echo '<h2>Human clicks by country</h2><table class="widefat striped" style="max-width:320px"><thead><tr><th>Country</th><th>Clicks</th></tr></thead><tbody>';
    foreach ( $countries as $r ) {
        printf( '<tr><td>%s</td><td>%d</td></tr>', esc_html( $r->country ?: '?' ), $r->n );
    }
    echo '</tbody></table>';

    $page_url = function ( $n ) use ( $days ) {
        return esc_url( add_query_arg( [ 'page' => 'roc-apply-clicks', 'days' => $days, 'paged' => $n ], admin_url( 'tools.php' ) ) );
    };
    $nav = sprintf( '<p>Page %d of %d (%d clicks) &nbsp; %s %s</p>', $paged, $pages, $total,
        $paged > 1 ? '<a class="button" href="' . $page_url( $paged - 1 ) . '">&laquo; Newer</a>' : '',
        $paged < $pages ? '<a class="button" href="' . $page_url( $paged + 1 ) . '">Older &raquo;</a>' : '' );
    echo '<h2 id="clicks">All clicks</h2>' . $nav . '<table class="widefat striped"><thead><tr><th>Time</th><th>Job</th><th>Partner</th><th>Outcome</th><th>Country</th><th>Referrer</th><th>User agent</th></tr></thead><tbody>';
    foreach ( $latest as $r ) {
        printf( '<tr><td>%s</td><td><a href="%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td style="font-size:11px">%s</td></tr>',
            esc_html( $r->created_at ), esc_url( get_permalink( $r->job_id ) ), esc_html( $r->post_title ?: '#' . $r->job_id ),
            esc_html( $r->source ), esc_html( $r->outcome ), esc_html( $r->country ), esc_html( $r->referer_host ), esc_html( $r->user_agent ) );
    }
    echo '</tbody></table>' . $nav . '</div>';
}

// ---------------------------------------------------------------------------
// 301 removed duplicate job URLs (Lensa dedup, 2026-09-26) to the copy that was kept
// ---------------------------------------------------------------------------
add_action( 'template_redirect', function () {
    if ( ! is_404() ) return;
    $path = (string) wp_parse_url( $_SERVER['REQUEST_URI'] ?? '', PHP_URL_PATH );
    if ( ! preg_match( '#^/job/([^/]+)/?$#', $path, $m ) ) return;

    global $wpdb;
    $table = $wpdb->prefix . 'roc_dup_redirects';
    if ( $wpdb->get_var( "SHOW TABLES LIKE '$table'" ) !== $table ) return;
    $target = (int) $wpdb->get_var( $wpdb->prepare( "SELECT target_id FROM $table WHERE slug = %s", $m[1] ) );
    if ( $target && get_post_status( $target ) ) {
        // get_permalink() gives ?p= URLs for expired posts; use the pretty job URL instead
        $url = get_post_status( $target ) === 'publish' ? get_permalink( $target )
             : home_url( '/job/' . get_post_field( 'post_name', $target ) . '/' );
        wp_safe_redirect( $url, 301, 'RevOpsCareers' );
        exit;
    }
}, 1 );


// ---------------------------------------------------------------------------
// Google Jobs: fix the JobPosting structured data WP Job Manager outputs
// - directApply: every apply link leads off-site (partner or employer ATS)
// - jobLocation: jobs are never geocoded, so WPJM falls back to the raw
//   "City, Region, Country" string; send a PostalAddress with addressCountry
// - remote jobs: mark "Remote" locations TELECOMMUTE and say which country
//   applicants must be in (Google requires this for remote jobs)
// ---------------------------------------------------------------------------
add_filter( 'wpjm_get_job_listing_structured_data', function ( $data, $post ) {
    $method = get_the_job_application_method( $post );
    if ( $method && $method->type === 'url' ) {
        $data['directApply'] = false;
    }

    $location = (string) get_post_meta( $post->ID, '_job_location', true );
    $parts    = array_values( array_filter( array_map( 'trim', explode( ',', $location ) ),
        fn( $p ) => $p !== '' && strcasecmp( $p, 'Remote' ) !== 0 ) );
    $country  = $parts ? end( $parts ) : '';
    // Prefer the real country (ISO code) over the raw last part ("DEU - Berlin", "Austin, TX")
    $real_country = roc_extract_country( $location );
    if ( $real_country ) {
        $country = $real_country;
        if ( $parts && roc_normalize_country( end( $parts ) ) !== $real_country ) {
            $parts = [ $real_country ];
        }
    }

    if ( $country && isset( $data['jobLocation'] ) && is_string( $data['jobLocation']['address'] ?? null ) ) {
        array_pop( $parts );
        $address = [ '@type' => 'PostalAddress', 'addressCountry' => $real_country ? roc_country_iso( $real_country ) : $country ];
        if ( count( $parts ) >= 2 ) {
            $address['addressLocality'] = $parts[0];
            $address['addressRegion']   = $parts[1];
        } elseif ( $parts ) {
            $address['addressRegion'] = $parts[0];
        }
        $data['jobLocation']['address'] = $address;
    }

    if ( $post->_remote_position || stripos( $location, 'remote' ) !== false ) {
        $data['jobLocationType'] = 'TELECOMMUTE';
        if ( $country ) {
            $data['applicantLocationRequirements'] = [ '@type' => 'Country', 'name' => $country ];
        }
    }

    return $data;
}, 10, 2 );


// ---------------------------------------------------------------------------
// Expired jobs: keep the page for visitors but out of Google's index (77K+
// pages with no job left; Google Jobs already drops them as the markup goes)
// ---------------------------------------------------------------------------
add_filter( 'the_seo_framework_robots_meta_array', function ( $meta, $args ) {
    if ( null === $args ) {
        if ( ! is_singular( 'job_listing' ) ) return $meta;
        $id = get_queried_object_id();
    } elseif ( empty( $args['tax'] ) && empty( $args['pta'] ) && empty( $args['uid'] ) ) {
        $id = (int) $args['id'];
    } else {
        return $meta;
    }
    if ( $id && get_post_type( $id ) === 'job_listing' && get_post_status( $id ) === 'expired' ) {
        $meta['noindex'] = 'noindex';
    }
    return $meta;
}, 10, 2 );

// ---------------------------------------------------------------------------
// WPJM Applications bug: it filters comments on wp_posts.post_type but skips its
// own join when WooCommerce has already joined wp_posts under an alias, so every
// comment query fails ("Unknown column 'wp_posts.post_type'"). Add the join back.
// ---------------------------------------------------------------------------
add_filter( 'comments_clauses', function ( $clauses ) {
    global $wpdb;
    if ( strpos( $clauses['where'], "{$wpdb->posts}.post_type NOT IN ('job_application')" ) !== false
        && ! preg_match( "/JOIN\s+{$wpdb->posts}\s+ON/", $clauses['join'] ) ) {
        $clauses['join'] .= " LEFT JOIN {$wpdb->posts} ON {$wpdb->comments}.comment_post_ID = {$wpdb->posts}.ID ";
    }
    return $clauses;
}, 100 );


// ---------------------------------------------------------------------------
// HubSpot for WooCommerce treats ANY ?code= on admin_init as its own OAuth
// callback, so it hijacks Site Kit's Google sign-in (and marks itself
// disconnected). Keep it out of Site Kit's callback (?oauth2callback=1).
// ---------------------------------------------------------------------------
add_action( 'admin_init', function () {
    if ( empty( $_GET['oauth2callback'] ) ) return;
    global $wp_filter;
    foreach ( $wp_filter['admin_init']->callbacks ?? [] as $priority => $callbacks ) {
        foreach ( $callbacks as $cb ) {
            if ( is_array( $cb['function'] ) && ( $cb['function'][1] ?? '' ) === 'hubwoo_redirect_from_hubspot' ) {
                remove_action( 'admin_init', $cb['function'], $priority );
            }
        }
    }
}, 0 );

// MailPoet: CAPTCHA only for repeat submitters (same IP/email within a month)
add_filter( "mailpoet_subscription_captcha_recipient_limit", function () { return 1; } );

/**
 * RevOpsCareers – [roc_job_categories] shortcode for the /job-categories/ page
 *
 * Appended to the child theme functions.php on the server.
 * Lists every non-empty job_listing_category alphabetically with its latest
 * jobs and a "View all" link, so new categories appear automatically.
 *
 * Rendered server-side (crawlable) and cached for 1 hour; the cache is also
 * cleared when a category is created, edited or deleted.
 *
 * Usage: [roc_job_categories per="5"]
 */

add_action( 'created_job_listing_category', 'roc_job_categories_bust_cache' );
add_action( 'edited_job_listing_category',  'roc_job_categories_bust_cache' );
add_action( 'delete_job_listing_category',  'roc_job_categories_bust_cache' );

function roc_job_categories_bust_cache() {
    delete_transient( 'roc_job_categories_v1' );
}

add_shortcode( 'roc_job_categories', function ( $atts ) {
    $per = max( 1, min( 10, (int) shortcode_atts( [ 'per' => 5 ], $atts )['per'] ) );

    $cached = get_transient( 'roc_job_categories_v1' );
    if ( is_array( $cached ) && ( $cached['per'] ?? 0 ) === $per ) {
        return $cached['html'];
    }

    $terms = get_terms( [
        'taxonomy'   => 'job_listing_category',
        'hide_empty' => true,
        'orderby'    => 'name',
        'order'      => 'ASC',
    ] );
    if ( is_wp_error( $terms ) || ! $terms ) {
        return '';
    }

    ob_start();
    ?>
<style>
#roc-catdir{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;max-width:1200px;margin:0 auto}
#roc-catdir .roc-catgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,340px),1fr));gap:16px}
#roc-catdir .roc-catcard{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:20px;display:flex;flex-direction:column;transition:box-shadow .2s}
#roc-catdir .roc-catcard:hover{box-shadow:0 4px 16px rgba(0,0,0,.08);border-color:#d1d5db}
#roc-catdir .roc-cathead{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px}
#roc-catdir .roc-catname{font-size:17px;font-weight:600;color:#111827;text-decoration:none;line-height:1.3}
#roc-catdir .roc-catname:hover{color:#6366f1}
#roc-catdir .roc-catbadge{flex-shrink:0;font-size:11px;padding:4px 10px;border-radius:5px;font-weight:500;background:#faf5ff;color:#7e22ce;border:1px solid #e9d5ff;white-space:nowrap}
#roc-catdir .roc-catjobs{list-style:none;margin:0 0 12px;padding:0;flex:1}
#roc-catdir .roc-catjobs li{padding:8px 0;border-top:1px solid #f3f4f6;margin:0}
#roc-catdir .roc-catjobs a{font-size:14px;font-weight:500;color:#1f2937;text-decoration:none;display:block;line-height:1.35}
#roc-catdir .roc-catjobs a:hover{color:#6366f1}
#roc-catdir .roc-catmeta{font-size:12px;color:#6b7280;margin-top:2px}
#roc-catdir .roc-catall{font-size:13px;font-weight:600;color:#6366f1;text-decoration:none}
#roc-catdir .roc-catall:hover{text-decoration:underline}
</style>
<div id="roc-catdir"><div class="roc-catgrid">
<?php
    foreach ( $terms as $term ) {
        $link = get_term_link( $term );
        if ( is_wp_error( $link ) ) {
            continue;
        }
        $jobs = get_posts( [
            'post_type'        => 'job_listing',
            'post_status'      => 'publish',
            'posts_per_page'   => $per,
            'orderby'          => 'date',
            'order'            => 'DESC',
            'no_found_rows'    => true,
            'suppress_filters' => false,
            'tax_query'        => [ [
                'taxonomy' => 'job_listing_category',
                'field'    => 'term_id',
                'terms'    => $term->term_id,
            ] ],
        ] );
        $count = (int) $term->count;
        ?>
<div class="roc-catcard">
  <div class="roc-cathead">
    <a class="roc-catname" href="<?php echo esc_url( $link ); ?>"><?php echo esc_html( $term->name ); ?></a>
    <span class="roc-catbadge"><?php echo esc_html( number_format_i18n( $count ) . ' job' . ( 1 === $count ? '' : 's' ) ); ?></span>
  </div>
  <ul class="roc-catjobs">
<?php
        foreach ( $jobs as $job ) {
            $meta = array_filter( [
                get_post_meta( $job->ID, '_company_name', true ),
                get_post_meta( $job->ID, '_job_location', true ),
                get_the_date( 'M j', $job ),
            ] );
            ?>
    <li><a href="<?php echo esc_url( get_permalink( $job ) ); ?>"><?php echo esc_html( get_the_title( $job ) ); ?></a>
      <div class="roc-catmeta"><?php echo esc_html( implode( ' · ', $meta ) ); ?></div></li>
<?php
        }
        ?>
  </ul>
  <a class="roc-catall" href="<?php echo esc_url( $link ); ?>">View all <?php echo esc_html( number_format_i18n( $count ) . ' ' . $term->name ); ?> jobs →</a>
</div>
<?php
    }
    ?>
</div></div>
<?php
    $html = ob_get_clean();
    set_transient( 'roc_job_categories_v1', [ 'per' => $per, 'html' => $html ], HOUR_IN_SECONDS );
    return $html;
} );

/**
 * RevOpsCareers – [roc_countries] shortcode for the /countries/ page
 *
 * Appended to the child theme functions.php on the server, after the
 * /roc/v1/countries endpoint (reuses roc_countries_endpoint() and its cache).
 * Renders the country cards server-side so they are crawlable; the search box
 * only filters the rendered cards.
 */

add_shortcode( 'roc_countries', function () {
    $data = rest_ensure_response( roc_countries_endpoint() )->get_data();
    if ( ! $data ) {
        return '';
    }

    ob_start();
    ?>
<style>
#roc-cdir{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;max-width:1200px;margin:0 auto;padding:0 16px}
#roc-csearch{width:100%;padding:12px 16px;font-size:16px;border:2px solid #e2e8f0;border-radius:8px;margin-bottom:16px;box-sizing:border-box;outline:none}
#roc-csearch:focus{border-color:#6366f1}
#roc-ccount{font-size:14px;color:#6b7280;margin-bottom:20px}
.roc-cgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:16px}
.roc-ccard{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:20px 16px;display:flex;flex-direction:column;align-items:center;text-align:center;transition:box-shadow .2s;text-decoration:none;color:inherit}
.roc-ccard:hover{box-shadow:0 4px 16px rgba(0,0,0,.08);border-color:#d1d5db}
.roc-cflag{font-size:48px;line-height:1;margin-bottom:10px}
.roc-cname{font-size:14px;font-weight:600;color:#111827;margin-bottom:8px;line-height:1.3}
.roc-cbadge{font-size:11px;padding:4px 10px;border-radius:5px;font-weight:500;background:#faf5ff;color:#7e22ce;border:1px solid #e9d5ff}
#roc-cempty{display:none;text-align:center;padding:40px;color:#9ca3af;font-size:16px}
</style>
<div id="roc-cdir">
  <input id="roc-csearch" type="text" placeholder="Search countries…">
  <div id="roc-ccount"><?php echo esc_html( count( $data ) . ' countries' ); ?></div>
  <div id="roc-cempty">No countries match your search.</div>
  <div id="roc-cbody" class="roc-cgrid">
<?php foreach ( $data as $c ) : ?>
    <a class="roc-ccard" href="<?php echo esc_url( $c['url'] ); ?>" data-name="<?php echo esc_attr( strtolower( $c['country'] ) ); ?>">
      <div class="roc-cflag"><?php echo esc_html( $c['flag'] ); ?></div>
      <div class="roc-cname"><?php echo esc_html( $c['country'] ); ?></div>
      <div class="roc-cbadge"><?php echo esc_html( number_format_i18n( $c['count'] ) . ' job' . ( 1 === (int) $c['count'] ? '' : 's' ) ); ?></div>
    </a>
<?php endforeach; ?>
  </div>
</div>
<script>
(function(){
  var input = document.getElementById('roc-csearch');
  var cards = document.querySelectorAll('#roc-cbody .roc-ccard');
  input.addEventListener('input', function(){
    var q = input.value.trim().toLowerCase(), n = 0;
    cards.forEach(function(c){
      var show = !q || c.dataset.name.indexOf(q) !== -1;
      c.style.display = show ? '' : 'none';
      if (show) n++;
    });
    document.getElementById('roc-ccount').textContent = n + ' countr' + (n === 1 ? 'y' : 'ies');
    document.getElementById('roc-cempty').style.display = n ? 'none' : '';
  });
})();
</script>
<?php
    return ob_get_clean();
} );

/**
 * RevOpsCareers – [roc_companies] shortcode for the /companies/ page
 *
 * Appended to the child theme functions.php on the server, after the
 * /roc/v1/companies endpoint (reuses roc_companies_endpoint()).
 *
 * Server-rendered, crawlable directory:
 *   /companies/                 top companies by job count
 *   /companies/?letter=b&pg=2   companies starting with B, alphabetical, paginated
 * The search box lazy-loads /wp-json/roc/v1/companies and searches all companies.
 *
 * The endpoint cache is cleared on every job save (thousands per day from imports)
 * and a cold build takes ~6 s, so the page keeps its own hourly copy.
 */

function roc_companies_page_data() {
    $data = get_transient( 'roc_companies_page_v1' );
    if ( false === $data ) {
        $data = rest_ensure_response( roc_companies_endpoint() )->get_data();
        set_transient( 'roc_companies_page_v1', $data, HOUR_IN_SECONDS );
    }
    return $data;
}

/** Current ?letter= ('a'–'z' or 'num' for non-letters), or '' for the top view. */
function roc_companies_letter() {
    $l = strtolower( sanitize_text_field( wp_unslash( $_GET['letter'] ?? '' ) ) );
    return preg_match( '/^([a-z]|num)$/', $l ) ? $l : '';
}

function roc_companies_first_letter( $name ) {
    $c = strtolower( substr( ltrim( $name ), 0, 1 ) );
    return ( $c >= 'a' && $c <= 'z' ) ? $c : 'num';
}

// Self-referencing canonical + distinct title for letter/page views.
add_filter( 'the_seo_framework_meta_render_data', function ( $tags ) {
    if ( ! is_page( 'companies' ) || ! roc_companies_letter() ) {
        return $tags;
    }
    $url = add_query_arg( array_filter( [
        'letter' => roc_companies_letter(),
        'pg'     => max( 1, (int) ( $_GET['pg'] ?? 1 ) ) > 1 ? (int) $_GET['pg'] : null,
    ] ), get_permalink() );
    if ( isset( $tags['canonical']['attributes'] ) ) {
        $tags['canonical']['attributes']['href'] = $url;
    }
    if ( isset( $tags['og:url']['attributes'] ) ) {
        $tags['og:url']['attributes']['content'] = $url;
    }
    return $tags;
} );

foreach ( [ 'the_seo_framework_title_from_custom_field', 'the_seo_framework_title_from_generation' ] as $roc_hook ) {
    add_filter( $roc_hook, function ( $title ) {
        if ( ! is_page( 'companies' ) || ! roc_companies_letter() ) {
            return $title;
        }
        $l    = roc_companies_letter();
        $pg   = max( 1, (int) ( $_GET['pg'] ?? 1 ) );
        $text = 'Companies Hiring – ' . ( 'num' === $l ? '#' : strtoupper( $l ) );
        return $text . ( $pg > 1 ? " (Page $pg)" : '' );
    } );
}

function roc_companies_card( $c ) {
    $name  = $c['company'] ?? '';
    $count = (int) $c['count'];
    $logo  = $c['logo_url']
        ? '<img decoding="async" loading="lazy" class="roc-logo" src="' . esc_url( $c['logo_url'] ) . '" alt="' . esc_attr( $name ) . '">'
        : '<div class="roc-initial">' . esc_html( strtoupper( substr( $name, 0, 1 ) ) ) . '</div>';
    $links = '<a class="roc-link roc-link-jobs" href="' . esc_url( $c['url'] ) . '">' . esc_html( number_format_i18n( $count ) . ' job' . ( 1 === $count ? '' : 's' ) ) . '</a>';
    if ( $c['website'] ) {
        $links .= '<a class="roc-link roc-link-web" href="' . esc_url( $c['website'] ) . '" target="_blank" rel="noopener nofollow">Website</a>';
    }
    if ( $c['linkedin'] ) {
        $links .= '<a class="roc-link roc-link-li" href="' . esc_url( $c['linkedin'] ) . '" target="_blank" rel="noopener nofollow">LinkedIn</a>';
    }
    return '<div class="roc-card">' . $logo . '<div class="roc-name"><a href="' . esc_url( $c['url'] ) . '">' . esc_html( $name ) . '</a></div><div class="roc-links">' . $links . '</div></div>';
}

add_shortcode( 'roc_companies', function ( $atts ) {
    $per    = max( 12, (int) shortcode_atts( [ 'per' => 120 ], $atts )['per'] );
    $data   = roc_companies_page_data();
    $total  = count( $data );
    $letter = roc_companies_letter();
    $base   = get_permalink();

    if ( $letter ) {
        $list = array_values( array_filter( $data, function ( $c ) use ( $letter ) {
            return roc_companies_first_letter( $c['company'] ?? '' ) === $letter;
        } ) );
        usort( $list, function ( $a, $b ) {
            return strnatcasecmp( $a['company'], $b['company'] );
        } );
        $pages = max( 1, (int) ceil( count( $list ) / $per ) );
        $pg    = min( $pages, max( 1, (int) ( $_GET['pg'] ?? 1 ) ) );
        $shown = array_slice( $list, ( $pg - 1 ) * $per, $per );
        $label = number_format_i18n( count( $list ) ) . ' compan' . ( 1 === count( $list ) ? 'y' : 'ies' )
               . ' starting with ' . ( 'num' === $letter ? 'a number or symbol' : strtoupper( $letter ) )
               . ( $pages > 1 ? " · page $pg of $pages" : '' );
    } else {
        $shown = array_slice( $data, 0, $per ); // endpoint is sorted by job count
        $pages = 1;
        $pg    = 1;
        $label = 'Top ' . count( $shown ) . ' of ' . number_format_i18n( $total ) . ' companies by open jobs · pick a letter to browse them all';
    }

    ob_start();
    ?>
<style>
#roc-dir{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;max-width:1200px;margin:0 auto;padding:0 16px}
#roc-search{width:100%;padding:12px 16px;font-size:16px;border:2px solid #e2e8f0;border-radius:8px;margin-bottom:16px;box-sizing:border-box;outline:none}
#roc-search:focus{border-color:#6366f1}
#roc-az{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:24px}
#roc-az a{padding:6px 10px;border:1px solid #f16736;border-radius:6px;background:#fff;font-size:13px;font-weight:500;color:#374151;text-decoration:none;transition:all .15s}
#roc-az a:hover{background:#f3f4f6}
#roc-az a.active{background:#f16736;border-color:#f16736;color:#fff}
.roc-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:16px}
.roc-card{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:16px;display:flex;flex-direction:column;align-items:center;text-align:center;transition:box-shadow .2s}
.roc-card:hover{box-shadow:0 4px 16px rgba(0,0,0,.08)}
.roc-logo{width:64px;height:64px;object-fit:contain;border-radius:8px;margin-bottom:10px}
.roc-initial{width:64px;height:64px;border-radius:8px;margin-bottom:10px;display:flex;align-items:center;justify-content:center;font-size:26px;font-weight:700;color:#fff;background:linear-gradient(135deg,#6366f1,#8b5cf6)}
.roc-name{font-size:14px;font-weight:600;color:#111827;margin-bottom:8px;line-height:1.3}
.roc-name a{color:inherit;text-decoration:none}
.roc-links{display:flex;gap:6px;flex-wrap:wrap;justify-content:center}
.roc-link{font-size:11px;padding:4px 8px;border-radius:5px;text-decoration:none;font-weight:500}
.roc-link-web{background:#f0fdf4;color:#15803d;border:1px solid #bbf7d0}
.roc-link-li{background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe}
.roc-link-jobs{background:#faf5ff;color:#7e22ce;border:1px solid #e9d5ff}
#roc-count{font-size:14px;color:#6b7280;margin-bottom:16px}
#roc-empty{display:none;text-align:center;padding:40px;color:#9ca3af;font-size:16px}
.roc-pages{display:flex;flex-wrap:wrap;gap:4px;justify-content:center;margin:28px 0 8px}
.roc-pages a,.roc-pages span{padding:6px 11px;border:1px solid #e5e7eb;border-radius:6px;font-size:13px;color:#374151;text-decoration:none}
.roc-pages span{background:#f16736;border-color:#f16736;color:#fff}
</style>
<div id="roc-dir">
  <input id="roc-search" type="text" placeholder="Search all <?php echo esc_attr( number_format_i18n( $total ) ); ?> companies…">
  <div id="roc-az">
    <a href="<?php echo esc_url( $base ); ?>"<?php echo $letter ? '' : ' class="active"'; ?>>Top</a>
<?php foreach ( array_merge( range( 'a', 'z' ), [ 'num' ] ) as $l ) : ?>
    <a href="<?php echo esc_url( add_query_arg( 'letter', $l, $base ) ); ?>"<?php echo $l === $letter ? ' class="active"' : ''; ?>><?php echo 'num' === $l ? '#' : strtoupper( $l ); ?></a>
<?php endforeach; ?>
  </div>
  <div id="roc-count"><?php echo esc_html( $label ); ?></div>
  <div id="roc-empty">No companies match your search.</div>
  <div id="roc-results" class="roc-grid" style="display:none"></div>
  <div id="roc-server">
    <div class="roc-grid">
<?php foreach ( $shown as $c ) { echo roc_companies_card( $c ); } ?>
    </div>
<?php if ( $pages > 1 ) : ?>
    <nav class="roc-pages" aria-label="Pages">
<?php for ( $i = 1; $i <= $pages; $i++ ) :
        if ( $i === $pg ) : ?>
      <span aria-current="page"><?php echo (int) $i; ?></span>
<?php   else : ?>
      <a href="<?php echo esc_url( add_query_arg( array_filter( [ 'letter' => $letter, 'pg' => $i > 1 ? $i : null ] ), $base ) ); ?>"><?php echo (int) $i; ?></a>
<?php   endif;
      endfor; ?>
    </nav>
<?php endif; ?>
  </div>
</div>
<script>
(function(){
  var DATA = null, loading = false, timer;
  var input = document.getElementById('roc-search');
  var count = document.getElementById('roc-count'), initial = count.textContent;
  var server = document.getElementById('roc-server'), results = document.getElementById('roc-results');
  var empty = document.getElementById('roc-empty');

  function esc(s){ return String(s||'').replace(/[&<>"']/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
  function card(c){
    var name = c.company || '';
    var logo = c.logo_url
      ? '<img decoding="async" loading="lazy" class="roc-logo" src="' + esc(c.logo_url) + '" alt="' + esc(name) + '">'
      : '<div class="roc-initial">' + esc(name.charAt(0).toUpperCase()) + '</div>';
    var links = '<a class="roc-link roc-link-jobs" href="' + esc(c.url) + '">' + c.count + ' job' + (c.count === 1 ? '' : 's') + '</a>';
    if (c.website)  links += '<a class="roc-link roc-link-web" href="' + esc(c.website) + '" target="_blank" rel="noopener nofollow">Website</a>';
    if (c.linkedin) links += '<a class="roc-link roc-link-li" href="' + esc(c.linkedin) + '" target="_blank" rel="noopener nofollow">LinkedIn</a>';
    return '<div class="roc-card">' + logo + '<div class="roc-name"><a href="' + esc(c.url) + '">' + esc(name) + '</a></div><div class="roc-links">' + links + '</div></div>';
  }
  function render(){
    var q = input.value.trim().toLowerCase();
    if (!q){ results.style.display = 'none'; empty.style.display = 'none'; server.style.display = ''; count.textContent = initial; return; }
    if (!DATA){
      count.textContent = 'Loading companies…';
      if (!loading){
        loading = true;
        fetch('/wp-json/roc/v1/companies').then(function(r){ return r.json(); })
          .then(function(d){ DATA = d; render(); })
          .catch(function(){ loading = false; count.textContent = 'Could not load companies. Please try again.'; });
      }
      return;
    }
    var found = DATA.filter(function(c){ return (c.company || '').toLowerCase().indexOf(q) !== -1; });
    server.style.display = 'none';
    count.textContent = found.length + ' compan' + (found.length === 1 ? 'y' : 'ies') + (found.length > 200 ? ' · showing first 200' : '');
    empty.style.display = found.length ? 'none' : '';
    results.style.display = found.length ? '' : 'none';
    results.innerHTML = found.slice(0, 200).map(card).join('');
  }
  input.addEventListener('input', function(){ clearTimeout(timer); timer = setTimeout(render, 150); });
})();
</script>
<?php
    return ob_get_clean();
} );

/**
 * RevOpsCareers – 301 deleted duplicate job URLs ("…-analyst-5") to the
 * surviving copy with the same base slug, when wp_roc_dup_redirects has no entry.
 *
 * Appended to the child theme functions.php; runs after the dup-table
 * handler (priority 1). Jobs with no surviving copy keep returning 404.
 */
add_action( 'template_redirect', function () {
    if ( ! is_404() ) return;
    $path = (string) wp_parse_url( $_SERVER['REQUEST_URI'] ?? '', PHP_URL_PATH );
    if ( ! preg_match( '#^/job/([a-z0-9-]+)/?$#', $path, $m ) ) return;

    global $wpdb;
    $base = preg_replace( '/-\d+$/', '', $m[1] );
    $id   = (int) $wpdb->get_var( $wpdb->prepare(
        "SELECT ID FROM {$wpdb->posts}
         WHERE post_type = 'job_listing' AND post_status IN ('publish','expired')
           AND ( post_name = %s OR post_name REGEXP %s )
         ORDER BY post_status = 'publish' DESC, post_date DESC LIMIT 1",
        $base, '^' . preg_quote( $base ) . '-[0-9]+$'
    ) );
    if ( $id && get_post_field( 'post_name', $id ) !== $m[1] ) {
        wp_safe_redirect( home_url( '/job/' . get_post_field( 'post_name', $id ) . '/' ), 301, 'RevOpsCareers' );
        exit;
    }
}, 3 );

/**
 * SEO: fix skipped heading levels (H2 → H4) — sidebar callout titles and blog card titles become H3.
 * Visual size is unchanged: both are styled by class, not by tag.
 */
function roc_h4_to_h3( $html, $class ) {
    return preg_replace( '#<h4 class="' . $class . '">(.*?)</h4>#s', '<h3 class="' . $class . '">$1</h3>', $html );
}
add_action( 'dynamic_sidebar_before', function() { ob_start(); } );
add_action( 'dynamic_sidebar_after', function() { echo roc_h4_to_h3( ob_get_clean(), 'ignition-box-callout-title' ); } );
add_filter( 'render_block_gutenbee/post-types', function( $html ) { return roc_h4_to_h3( $html, 'entry-item-title' ); } );

/**
 * Job search keyword synonyms: a search for "gtm" (e.g. the /gtm-jobs/ page shortcode)
 * also matches listings that say "go-to-market" or "go to market" in the title or description.
 * WP Job Manager ANDs search words, so the OR has to be built here.
 */
function roc_job_keyword_synonyms( $search, $query ) {
    global $job_manager_keyword, $wpdb;
    $synonyms = array(
        'gtm' => array( 'gtm', 'go-to-market', 'go to market' ),
    );
    $keyword = strtolower( trim( (string) $job_manager_keyword ) );
    if ( ! isset( $synonyms[ $keyword ] ) || 'job_listing' !== $query->get( 'post_type' ) || '' === (string) $query->get( 's' ) ) {
        return $search;
    }
    $or = array();
    foreach ( $synonyms[ $keyword ] as $term ) {
        $like = '%' . $wpdb->esc_like( $term ) . '%';
        $or[] = $wpdb->prepare( "{$wpdb->posts}.post_title LIKE %s OR {$wpdb->posts}.post_content LIKE %s", $like, $like );
    }
    return ' AND (' . implode( ' OR ', $or ) . ') ';
}
add_filter( 'posts_search', 'roc_job_keyword_synonyms', 20, 2 );

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

/**
 * ReLoop's embed (title, lines, AI label + link) laid out as paragraphs instead of one bullet list.
 * ReLoop's wording is kept as is; only "a · b · c" lists are joined into sentences.
 */
function roc_reloop_read_html( $embed ) {
    $e      = 'esc_html';
    $lower  = function ( $t ) { return preg_match( '/^[A-Z][a-z]/', $t ) ? lcfirst( $t ) : $t; };   // keep "SQL", "HubSpot"
    $items  = function ( $v ) { return array_values( array_filter( array_map( 'trim', explode( '·', $v ) ) ) ); };
    $facts  = [];
    $paras  = [];
    $skip   = array_keys( roc_quick_card_lines() ?? [] );   // shown in the sidebar Quick Role Check card instead
    foreach ( $embed['lines'] as $l ) {
        $label = $e( rtrim( $l['label'], ': ' ) );
        $value = trim( (string) $l['value'] );
        if ( $value === '' || in_array( $l['key'], $skip, true ) ) continue;
        if ( $l['key'] === 'bigger_picture' ) {
            $paras[] = $e( $value );
        } elseif ( in_array( $l['key'], [ 'level', 'title_vs_scope', 'shape', 'inflation' ], true ) ) {
            $facts[] = '<strong>' . $label . ':</strong> ' . $e( $value ) . '.';
        } elseif ( $l['key'] === 'capabilities' ) {
            $list = $items( $value );
            $list = array_merge( [ $list[0] ], array_map( $lower, array_slice( $list, 1 ) ) );
            $last = array_pop( $list );
            $paras[] = '<strong>' . $label . ':</strong> ' . $e( $list ? implode( ', ', $list ) . ' and ' . $last : $last ) . '.';
        } elseif ( $l['key'] === 'not_for_you_if' ) {
            $paras[] = '<strong>' . $label . '</strong> ' . $e( implode( ', or ', array_map( $lower, $items( $value ) ) ) ) . '.';
        } else {
            $paras[] = '<strong>' . $label . ':</strong> ' . $e( $value );
        }
    }
    if ( $facts ) array_splice( $paras, 1, 0, [ implode( ' ', $facts ) ] );   // right after "Read between the lines"
    preg_match( '#<div style="font-size:12px[^"]*">(.*?)</div>#s', (string) $embed['html'], $m );   // ReLoop's AI label + link
    $foot = $m[1] ?? 'Indicative, AI-generated read, no salary range. <a href="https://www.reloop.me/role-intel?utm_source=revopscareers&utm_medium=job_page&utm_campaign=revopscareers&utm_content=role_read">ReLoop.me</a>';

    $h  = '<section class="roc-reloop-read" aria-label="' . esc_attr( $embed['title'] ) . '">';
    $h .= '<h3>' . $e( $embed['title'] ) . '</h3>';
    foreach ( $paras as $p ) $h .= '<p>' . $p . '</p>';
    $h .= '<p class="roc-rl-foot">' . wp_kses_post( $foot ) . '</p></section>';
    return $h;
}

add_filter( 'the_job_description', function ( $content ) {
    if ( ! is_singular( 'job_listing' ) ) return $content;
    $reloop = json_decode( (string) get_post_meta( get_the_ID(), '_roc_reloop_read', true ), true );
    if ( empty( $reloop['embed']['html'] ) || empty( $reloop['embed']['lines'] ) ) return $content;   // ReLoop decides what to show
    return $content . roc_reloop_read_html( $reloop['embed'] );
}, 9 );

add_action( 'wp_head', function () {
    if ( ! is_singular( 'job_listing' ) ) return;
    echo '<style>
.roc-reloop-read{border:1px solid #e3e6ea;border-left:4px solid #f16736;border-radius:6px;padding:18px 22px 8px;margin:28px 0 20px;background:#fafbfc}
.roc-reloop-read h3{margin:0 0 10px}.roc-reloop-read p{margin:0 0 12px}.roc-rl-foot{font-size:.8rem;color:#5f6b7a}
</style>';
} );

// ---------------------------------------------------------------------------
// Quick Role Check card in the single job sidebar, above Job Categories (ReLoop's spec, 2026-11).
// On/off switch: `wp option update roc_quick_card 1` (0 = off). When on, on jobs with a ReLoop read the
// card replaces ReLoop's sidebar callout (widget ignition-callout-12), and its lines are left out of the
// "Read by ReLoop.me" block under the description. Jobs without a read keep the callout.
// ---------------------------------------------------------------------------
/** The card's lines (level, title_vs_scope, inflation, not_for_you_if => value), or null when it doesn't show. */
function roc_quick_card_lines() {
    if ( ! get_option( 'roc_quick_card' ) || ! is_singular( 'job_listing' ) ) return null;
    $read = json_decode( (string) get_post_meta( get_queried_object_id(), '_roc_reloop_read', true ), true );
    if ( empty( $read['embed']['html'] ) || empty( $read['embed']['lines'] ) ) return null;
    $lines = array_column( $read['embed']['lines'], 'value', 'key' );
    $keys  = array_flip( [ 'level', 'title_vs_scope', 'inflation', 'not_for_you_if' ] );
    return array_filter( array_map( 'trim', array_intersect_key( $lines, $keys ) ) ) ?: null;
}

add_action( 'ignition_sidebar_before', function () {
    $q = roc_quick_card_lines();
    if ( ! $q ) return;
    $e   = 'esc_html';
    $url = esc_url( 'https://www.reloop.me/role-intel?utm_source=revopscareers&utm_medium=job_page&utm_campaign=revopscareers&utm_content=quick_sidebar' );
    echo '<aside class="widget roc-qrc"><div class="roc-qrc-label">Quick Role Check</div>'
       . '<h3 class="roc-qrc-title">Is this role what it says?</h3>';
    foreach ( [ 'level' => 'Level', 'title_vs_scope' => 'Title vs scope', 'inflation' => 'Requirement inflation' ] as $k => $label ) {
        if ( isset( $q[ $k ] ) ) echo '<div class="roc-qrc-row"><span>' . $label . '</span><span class="roc-qrc-pill">' . $e( $q[ $k ] ) . '</span></div>';
    }
    if ( isset( $q['not_for_you_if'] ) ) {
        $items = array_filter( array_map( 'trim', explode( '·', $q['not_for_you_if'] ) ) );
        $items = array_map( fn( $t ) => preg_match( '/^[A-Z][a-z]/', $t ) ? lcfirst( $t ) : $t, $items );   // keep "SQL", "HubSpot"
        echo '<p class="roc-qrc-nfy"><strong>Not for you if</strong> ' . $e( implode( ', or ', $items ) ) . '.</p>';
    }
    echo '<p class="roc-qrc-note">Indicative, AI-generated read, no salary range. <a href="' . $url . '" target="_blank" rel="noopener">Read by ReLoop.me</a></p>'
       . '<div class="roc-qrc-up"><strong>Get the full Role Intel for this job</strong>'
       . '<p>The real level, pay where the market is covered, a checklist and the questions to ask before you apply.</p>'
       . '<a class="roc-qrc-btn" href="' . $url . '" target="_blank" rel="noopener">Get Role Intel</a></div></aside>';
}, 22 );   // after Job Information (21), before the widgets

add_filter( 'widget_display_callback', function ( $instance, $widget ) {
    return ( $widget->id === 'ignition-callout-12' && roc_quick_card_lines() ) ? false : $instance;
}, 10, 2 );

add_action( 'wp_head', function () {
    if ( ! roc_quick_card_lines() ) return;
    echo '<style>
.roc-qrc{background:#fff;border:1px solid #e7a23f;border-radius:12px;padding:16px;box-shadow:inset 0 4px 0 #c99546;color:#14181a}
.roc-qrc-label{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.12em;color:#c99546;margin:4px 0}
.roc-qrc .roc-qrc-title{font-size:18px;font-weight:600;line-height:1.3;color:#14181a;margin:0 0 12px;text-transform:none;letter-spacing:0}
.roc-qrc-row{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:center;gap:4px 8px;font-size:14px;margin:0 0 8px}
.roc-qrc-pill{margin-left:auto;background:#fbf3e4;border-radius:999px;padding:2px 10px;font-size:13px}
.roc-qrc-nfy{font-size:14px;margin:12px 0 10px}
.roc-qrc-note{font-size:12px;color:#4a6360;margin:0}.roc-qrc .roc-qrc-note a{color:#4a6360;text-decoration:underline}
.roc-qrc-up{display:flex;flex-direction:column;border-top:1px solid #efe3cf;margin-top:14px;padding-top:14px;font-size:14px}
.roc-qrc-up p{margin:4px 0 12px}
.roc-qrc a.roc-qrc-btn,.roc-qrc a.roc-qrc-btn:hover{align-self:flex-end;background:#c99546;color:#fff;border-radius:999px;padding:8px 18px;font-weight:600;text-decoration:none}
.roc-qrc a.roc-qrc-btn:hover{background:#b5833a}
</style>';
} );

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

// ---------------------------------------------------------------------------
// WP Job Manager counts job_apply_click only on the first input.application_button (the button in
// the main column). The sidebar "Apply for this job" link (a.application_button, theme widget) was
// not counted. Log the same stat through WPJM's own script; its unique key (job_apply_click_<id>,
// 24 h in localStorage) keeps a visitor who clicks both buttons at one click.
// ---------------------------------------------------------------------------
add_action( 'wp_footer', function () {
    if ( ! is_singular( 'job_listing' ) ) return;
    echo "<script>document.addEventListener('click',function(e){
if(!e.target.closest('a.application_button')||!window.WPJMStats)return;
var s=(window.WPJMStats.stats||[]).find(function(x){return x.name==='job_apply_click';});
if(s)window.WPJMStats.log(s);});</script>";
} );
