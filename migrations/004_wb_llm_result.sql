-- 1) Cache seller name/trademark (static-basket)
create table if not exists wb_supplier_name_cache (
  supplier_id bigint primary key,
  trademark text,
  supplier_name text,
  supplier_full_name text,
  raw jsonb not null,
  fetched_at timestamptz not null default now()
);

-- 2) Cache seller rating/profile (suppliers-shipment-2)
create table if not exists wb_supplier_profile_cache (
  supplier_id bigint primary key,
  valuation real,
  feedbacks_count integer,
  registration_date timestamptz,
  sale_item_quantity bigint,
  rating real,
  supp_ratio integer,
  ratio_mark_supp integer,
  delivery_duration real,
  raw jsonb not null,
  fetched_at timestamptz not null default now()
);

create index if not exists idx_wb_supplier_profile_cache_val
  on wb_supplier_profile_cache (valuation desc, feedbacks_count desc);

-- 3) Black/white lists by exact seller name (your rule)
create table if not exists wb_supplier_name_lists (
  list_name text not null,               -- 'blacklist' | 'whitelist'
  exact_name text not null,              -- exact match (normalized in code)
  note text,
  created_at timestamptz not null default now(),
  primary key (list_name, exact_name)
);

-- 4) suppliers which cant be parsed through my endpoint on hand check

create table if not exists wb_supplier_unparsed (
  supplier_id bigint not null,
  nm_id bigint not null,
  card_url text not null,
  reason text not null,
  http_status integer,
  created_at timestamptz not null default now(),
  primary key (supplier_id, nm_id)
);

create index if not exists idx_wb_supplier_unparsed_created
  on wb_supplier_unparsed (created_at desc);

-- 5) LLM results with versioning + statuses
create table if not exists wb_llm_result (
  nm_id bigint not null,
  llm_ver integer not null,
  status text not null,                  -- new|supplier_reject|llm_done|error
  supplier_decision text,                -- pass|reject|whitelist_pass|blacklist_reject
  supplier_reason text,

  decision text,                         -- accept|reject|review|accept_unknown
  category text,
  target text,
  confidence real,
  reject_reason text,

  signals jsonb,
  explain jsonb,

  model text,
  attempts integer not null default 0,
  last_error text,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  primary key (nm_id, llm_ver)
);

create index if not exists idx_wb_llm_result_status on wb_llm_result (status, llm_ver);
create index if not exists idx_wb_llm_result_decision on wb_llm_result (decision, llm_ver);