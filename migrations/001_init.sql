CREATE TYPE agent_state AS ENUM
    ('OFFLINE','AVAILABLE','RESERVED','DIALING','CONNECTED','WRAP_UP','PAUSED');

CREATE TYPE call_state AS ENUM
    ('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED',
     'COMPLETED','FAILED','CANCELLED','ABANDONED');

CREATE TYPE borrower_state AS ENUM
    ('PENDING','LOCKED','IN_CALL','DONE','EXHAUSTED','SUPPRESSED');

CREATE TYPE pacing_mode AS ENUM ('PROGRESSIVE','PREDICTIVE');

-- ---------------------------------------------------------------- campaigns
CREATE TABLE campaigns (
    id                  BIGSERIAL PRIMARY KEY,
    name                TEXT        NOT NULL,
    mode                pacing_mode NOT NULL DEFAULT 'PROGRESSIVE',
    is_active           BOOLEAN     NOT NULL DEFAULT true,
    max_overdial_ratio  NUMERIC(4,2) NOT NULL DEFAULT 1.50,  -- hard cap, floor-guarded
    max_abandon_rate    NUMERIC(4,3) NOT NULL DEFAULT 0.030, -- 3% regulatory-style budget
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------- agents
CREATE TABLE agents (
    id                BIGSERIAL PRIMARY KEY,
    campaign_id       BIGINT      NOT NULL REFERENCES campaigns(id),
    ext_ref           TEXT        NOT NULL,
    state             agent_state NOT NULL DEFAULT 'OFFLINE',
    version           BIGINT      NOT NULL DEFAULT 0,     -- optimistic CAS token
    lease_owner       TEXT,                               -- worker id holding it
    lease_expires_at  TIMESTAMPTZ,
    current_call_id   BIGINT,
    state_changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, ext_ref)
);

-- the hot path for allocation: partial index keeps it tiny
CREATE INDEX idx_agents_available
    ON agents (campaign_id, updated_at)
    WHERE state = 'AVAILABLE';

-- the hot path for the reaper
CREATE INDEX idx_agents_leases
    ON agents (lease_expires_at)
    WHERE state IN ('RESERVED','DIALING');

-- ---------------------------------------------------------------- borrowers
CREATE TABLE borrowers (
    id               BIGSERIAL PRIMARY KEY,
    campaign_id      BIGINT         NOT NULL REFERENCES campaigns(id),
    phone            TEXT           NOT NULL,
    priority         INT            NOT NULL DEFAULT 100,  -- lower dials first
    state            borrower_state NOT NULL DEFAULT 'PENDING',
    attempt_count    INT            NOT NULL DEFAULT 0,
    max_attempts     INT            NOT NULL DEFAULT 3,
    next_eligible_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    locked_by        TEXT,
    locked_until     TIMESTAMPTZ,
    UNIQUE (campaign_id, phone)
);

CREATE INDEX idx_borrowers_dialable
    ON borrowers (campaign_id, priority, next_eligible_at)
    WHERE state = 'PENDING';

-- -------------------------------------------------------------------- calls
CREATE TABLE calls (
    id               BIGSERIAL PRIMARY KEY,
    campaign_id      BIGINT      NOT NULL REFERENCES campaigns(id),
    agent_id         BIGINT      REFERENCES agents(id),
    borrower_id      BIGINT      NOT NULL REFERENCES borrowers(id),
    provider         TEXT        NOT NULL,
    provider_call_id TEXT,
    state            call_state  NOT NULL DEFAULT 'QUEUED',
    state_rank       SMALLINT    NOT NULL DEFAULT 0,   -- denormalised for the guard
    attempt_no       INT         NOT NULL DEFAULT 1,
    idempotency_key  TEXT        NOT NULL,             -- campaign:borrower:attempt
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    initiated_at     TIMESTAMPTZ,
    ringing_at       TIMESTAMPTZ,
    answered_at      TIMESTAMPTZ,
    connected_at     TIMESTAMPTZ,
    terminal_at      TIMESTAMPTZ,
    failure_reason   TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (idempotency_key)                            -- retry cannot double-dial
);

CREATE UNIQUE INDEX idx_calls_provider_id
    ON calls (provider, provider_call_id)
    WHERE provider_call_id IS NOT NULL;

CREATE INDEX idx_calls_inflight
    ON calls (campaign_id, state)
    WHERE state IN ('RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED');

CREATE INDEX idx_calls_leases
    ON calls (lease_expires_at)
    WHERE terminal_at IS NULL;

-- ------------------------------------------------- provider event ledger
CREATE TABLE provider_events (
    id                BIGSERIAL PRIMARY KEY,
    provider          TEXT        NOT NULL,
    provider_event_id TEXT        NOT NULL,
    provider_call_id  TEXT,
    call_id           BIGINT      REFERENCES calls(id),
    event_type        TEXT        NOT NULL,
    provider_ts       TIMESTAMPTZ,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied           BOOLEAN     NOT NULL DEFAULT false,
    anomaly           TEXT,        -- NULL | 'DUPLICATE' | 'OUT_OF_ORDER' | 'UNKNOWN_CALL'
    payload           JSONB       NOT NULL DEFAULT '{}',
    UNIQUE (provider, provider_event_id)                -- exactly-once application
);

CREATE INDEX idx_events_unapplied
    ON provider_events (received_at)
    WHERE applied = false;

-- --------------------------------------------------- pacing audit trail
CREATE TABLE pacing_decisions (
    id                BIGSERIAL PRIMARY KEY,
    campaign_id       BIGINT      NOT NULL REFERENCES campaigns(id),
    tick_at           TIMESTAMPTZ NOT NULL,
    sim_tick          BIGINT,                 -- virtual clock tick, for replay
    mode              pacing_mode NOT NULL,
    requested         INT         NOT NULL,
    approved          INT         NOT NULL,
    reason_code       TEXT        NOT NULL,
    inputs            JSONB       NOT NULL    -- full MetricsSnapshot + estimator internals
);

CREATE INDEX idx_decisions_campaign_time ON pacing_decisions (campaign_id, tick_at DESC);

-- ------------------------------------------- O(1) counters (see §15 scale)
CREATE TABLE campaign_counters (
    campaign_id       BIGINT PRIMARY KEY REFERENCES campaigns(id),
    agents_available  INT NOT NULL DEFAULT 0,
    agents_reserved   INT NOT NULL DEFAULT 0,
    agents_dialing    INT NOT NULL DEFAULT 0,
    agents_connected  INT NOT NULL DEFAULT 0,
    agents_wrapup     INT NOT NULL DEFAULT 0,
    calls_ringing     INT NOT NULL DEFAULT 0,
    calls_connected   INT NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
