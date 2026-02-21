-- User Journey Query - Optimized Version
-- Identifies sessions that visited access pages and tracks all events in those sessions
-- Includes purchase conversion tracking and user identity resolution

WITH all_events AS (
  -- Union all event sources
  SELECT
    'page' AS source,
    name AS event_name,
    NULL AS event_value,
    timestamp,
    anonymous_id,
    context_session_id,
    user_id,
    context_traits_email AS email,
    COALESCE(branch_link, context_traits_branch_link) AS branch_link
  FROM `lissen-datalake-prod.app_events.pages`

  UNION ALL

  SELECT
    'screen' AS source,
    name AS event_name,
    NULL AS event_value,
    timestamp,
    anonymous_id,
    context_session_id,
    user_id,
    context_traits_email AS email,
    branch_link
  FROM `lissen-datalake-prod.app_events.screens`

  UNION ALL

  SELECT
    'press' AS source,
    event AS event_name,
    value AS event_value,
    timestamp,
    anonymous_id,
    context_session_id,
    user_id,
    context_traits_email AS email,
    COALESCE(branch_link, context_traits_branch_link) AS branch_link
  FROM `lissen-datalake-prod.app_events.press`
),

-- Filter for access page/screen events only
page_events AS (
  SELECT
    source,
    event_name,
    event_value,
    timestamp,
    anonymous_id,
    context_session_id,
    user_id,
    email,
    branch_link,
    SPLIT(event_name, '/')[SAFE_OFFSET(2)] AS resource_id  -- Extract resource_id from /access/<resource_id>
  FROM all_events
  WHERE
    source IN ('page', 'screen')
    -- AND event_name LIKE '/access/%'
),

-- Find nearest event with non-null session_id for session resolution
-- Window: 120 seconds (2 minutes) before/after the access event
candidates AS (
  SELECT
    b.source,
    b.event_name,
    b.event_value,
    b.timestamp,
    b.anonymous_id,
    b.context_session_id,
    b.user_id,
    b.email,
    b.branch_link,
    b.resource_id,
    n.context_session_id AS neighbor_session_id,
    ABS(TIMESTAMP_DIFF(n.timestamp, b.timestamp, SECOND)) AS diff_sec,
    ROW_NUMBER() OVER (
      PARTITION BY b.source, b.event_name, b.timestamp, b.anonymous_id
      ORDER BY ABS(TIMESTAMP_DIFF(n.timestamp, b.timestamp, SECOND))
    ) AS rn
  FROM page_events b
  LEFT JOIN all_events n
    ON n.anonymous_id = b.anonymous_id
    AND n.context_session_id IS NOT NULL
    AND ABS(TIMESTAMP_DIFF(n.timestamp, b.timestamp, SECOND)) <= 120
),

-- Resolve session IDs using nearest neighbor or existing session_id
page_events_with_session AS (
  SELECT
    source,
    event_name,
    event_value,
    timestamp,
    anonymous_id,
    context_session_id,
    user_id,
    email,
    branch_link,
    resource_id,
    COALESCE(context_session_id, neighbor_session_id) AS session_id_filled
  FROM candidates
  WHERE
    neighbor_session_id IS NULL  -- No neighbour found: keep row as-is
    OR rn = 1                     -- Or use closest neighbour within 120s window
),

-- Aggregate to one row per session that visited any access page
sessions AS (
  SELECT
    session_id_filled AS session_id,
    ANY_VALUE(anonymous_id) AS anonymous_id,
    MIN(timestamp) AS session_start,
    MAX(timestamp) AS session_end,
    ANY_VALUE(email) AS analytics_email,
    ANY_VALUE(branch_link) AS branch_link,
    ANY_VALUE(resource_id) AS resource_id
  FROM page_events_with_session
  WHERE session_id_filled IS NOT NULL
  GROUP BY session_id_filled
),

-- Get all successful buy_ticket events for access resources
buy_ticket_access AS (
  SELECT
    id,
    timestamp,
    anonymous_id,
    context_session_id,
    user_id,
    context_traits_email AS email,
    resource_id,
    resource_type
  FROM `lissen-datalake-prod.app_events.buy_ticket`
  WHERE resource_type = 'access'
),

-- Join sessions with purchase events
-- Match on session_id OR (anonymous_id + timestamp within window)
-- Window: 5 min before session start to 30 min after session end
sessions_with_buys AS (
  SELECT
    s.session_id,
    s.anonymous_id,
    s.session_start,
    s.session_end,
    s.branch_link,
    s.resource_id,
    s.analytics_email,
    b.id AS buy_ticket_id,
    b.timestamp AS buy_timestamp,
    b.user_id AS analytics_user_id,
    b.email AS buy_email
  FROM sessions s
  LEFT JOIN buy_ticket_access b
    ON (b.context_session_id = s.session_id)
    OR (
      b.context_session_id IS NULL
      AND b.anonymous_id = s.anonymous_id
      AND b.timestamp BETWEEN TIMESTAMP_SUB(s.session_start, INTERVAL 5 MINUTE)
                          AND TIMESTAMP_ADD(s.session_end, INTERVAL 30 MINUTE)
    )
),

-- User identity mappings from Postgres exports
export_map AS (
  SELECT
    id AS export_user_id,
    email AS export_email,
    anonymous_id
  FROM `lissen-datalake-prod.pg_manual_exports.users_anonymous_ids_29Nov`
),

-- Branch link creation events with creator information
create_branch_link_events AS (
  SELECT
    c.link AS branch_link,
    COALESCE(c.branch_link, c.context_traits_branch_link) AS source_branch_link,
    c.anonymous_id,
    COALESCE(c.user_id, c.context_traits_user_id, c.context_traits_id) AS creator_analytics_user_id,
    c.context_traits_email AS creator_analytics_email
  FROM `lissen-datalake-prod.app_events.create_branch_link` c
),

-- Resolve branch link creator identity
branch_link_creator AS (
  SELECT
    c.branch_link,
    c.source_branch_link,
    COALESCE(e.export_user_id, c.creator_analytics_user_id) AS branch_link_creator_user_id,
    COALESCE(e.export_email, c.creator_analytics_email) AS branch_link_creator_email
  FROM create_branch_link_events c
  LEFT JOIN export_map e ON e.anonymous_id = c.anonymous_id
),

-- Get all events for identified sessions
all_session_events AS (
  SELECT
    ae.context_session_id,
    ae.source,
    lower(ae.event_name) AS  event_name,
    ae.event_value,
    ae.timestamp,
    ae.anonymous_id,
    ae.user_id,
    ae.email,
    ae.branch_link,
    -- Session metadata
    sb.session_id,
    sb.anonymous_id AS session_anonymous_id,
    sb.session_start,
    sb.session_end,
    sb.resource_id,
    sb.branch_link AS session_branch_link,
    sb.buy_ticket_id,
    sb.buy_timestamp,
    sb.analytics_email AS session_email,
    -- Unified user identity (prioritize export map)
    COALESCE(em.export_user_id, sb.analytics_user_id) AS session_user_id,
    -- Branch link creator information
    blc.branch_link_creator_user_id,
    blc.branch_link_creator_email
  FROM all_events ae
  INNER JOIN sessions_with_buys sb
    ON ae.context_session_id = sb.session_id
    AND ae.anonymous_id = sb.anonymous_id
  LEFT JOIN export_map em ON em.anonymous_id = ae.anonymous_id
  LEFT JOIN branch_link_creator blc ON blc.branch_link = ae.branch_link
)

-- Final output: User journey with event categorization
SELECT
  context_session_id AS session_id,
  ROW_NUMBER() OVER (PARTITION BY context_session_id ORDER BY timestamp) AS event_sequence,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp) AS event_time,
  source,
  event_name,
  COALESCE(event_value, '') AS event_value,
  anonymous_id,
  user_id,
  email,
  session_user_id,
  session_email,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', session_start) AS session_start,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', session_end) AS session_end,
  resource_id,
  branch_link,
  CASE WHEN buy_ticket_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS converted_to_purchase,
  -- Event categorization for analysis
  CASE
    WHEN event_value = 'AccessCheckout.Pay' THEN 'access_checkout_pay'
    WHEN event_name = '/' THEN 'load'
    WHEN event_name LIKE '%onboarding%' THEN 'onboarding'
    WHEN event_name = '/search' THEN 'search'
    WHEN event_name = '/songs/access' THEN 'explore'
    WHEN event_name = '/exclusives' THEN 'feed'
    WHEN event_name LIKE '/access/%/checkout' THEN 'access_checkout_page'
    WHEN event_name LIKE '/access/%' THEN 'access_page'
    WHEN event_name = '/set-password' THEN 'password_page'
    WHEN event_name = 'PASSWORD_SUBMIT' THEN 'password_submit'
    WHEN event_name = 'buy_ticket' THEN 'Purchase'
    WHEN source = 'press' THEN 'Interaction'
    ELSE event_name
  END AS event_category,
CASE
    WHEN event_name = '/onboarding' THEN '1.onboarding_main'
    WHEN event_name = '/onboarding/intro' THEN '1.onboarding_intro'
    WHEN event_name = '/onboarding/loading' THEN '1.onboarding_loading'
    WHEN event_name like '%/onboarding/pick-genres%' THEN '1.onboarding_pick_genres'
    WHEN event_name = '/onboarding/pick-artists' OR event_name LIKE '%artistonboarding%' THEN '1.onboarding_pick_artists'
    WHEN event_name like '%onboarding/link-streaming-service%' THEN '1.onboarding_link_streaming'
    WHEN event_name like '%onboarding%callback%' THEN '1.onboarding_callback'
    WHEN event_name LIKE '%onboarding%close%' THEN '1.onboarding_close'
    WHEN event_name = '/search' THEN '3.search'
    WHEN event_name = '/songs/access' THEN '5.explore'
    WHEN event_name = '/exclusives' THEN '6.feed'
    WHEN event_name LIKE '/access/%/checkout' THEN '7.access_checkout_page'
    WHEN event_name LIKE '/access/%' THEN '4.access_page'
    WHEN event_name like '%password%' THEN '2.password_submit'
    WHEN event_name = 'buy_ticket' THEN '9.buy_ticket'
    WHEN event_value = 'AccessCheckout.Pay' THEN '8.access_checkout_pay'
    WHEN event_name = '/' THEN 'load'
    ELSE event_name
  END AS event_category_ordered,
  CASE
    WHEN event_value = 'AccessCheckout.Pay' THEN 8
    WHEN event_name LIKE '%onboarding%' THEN 1
    WHEN event_name = '/search' THEN 3
    WHEN event_name = '/songs/access' THEN 5
    WHEN event_name = '/exclusives' THEN 6
    WHEN event_name LIKE '/access/%/checkout' THEN 7
    WHEN event_name LIKE '/access/%' THEN 4
    WHEN event_name = '/set-password' THEN 2
    WHEN event_name = 'PASSWORD_SUBMIT' THEN 3
    WHEN event_name = 'buy_ticket' THEN 9
    ELSE NULL
  END AS event_category_ordered_numbered
FROM all_session_events
WHERE event_name LIKE '%onboarding%' -- Filter for onboarding events - funnel dashboard
ORDER BY session_id, timestamp;
