-- O(1) counters, maintained incrementally by triggers inside the same transaction
-- as each state change (see ARCHITECTURE.md §6, §15.1). This is the Phase 12 fix:
-- the pacing snapshot becomes a single-row primary-key read instead of a
-- `GROUP BY state` scan, regardless of agent/call count.

CREATE OR REPLACE FUNCTION trg_agents_counters() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO campaign_counters (campaign_id) VALUES (NEW.campaign_id)
            ON CONFLICT (campaign_id) DO NOTHING;
        UPDATE campaign_counters SET
            agents_available = agents_available + (CASE WHEN NEW.state = 'AVAILABLE' THEN 1 ELSE 0 END),
            agents_reserved  = agents_reserved  + (CASE WHEN NEW.state = 'RESERVED'  THEN 1 ELSE 0 END),
            agents_dialing   = agents_dialing   + (CASE WHEN NEW.state = 'DIALING'   THEN 1 ELSE 0 END),
            agents_connected = agents_connected + (CASE WHEN NEW.state = 'CONNECTED' THEN 1 ELSE 0 END),
            agents_wrapup    = agents_wrapup    + (CASE WHEN NEW.state = 'WRAP_UP'   THEN 1 ELSE 0 END),
            updated_at = now()
        WHERE campaign_id = NEW.campaign_id;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.state IS DISTINCT FROM NEW.state THEN
            UPDATE campaign_counters SET
                agents_available = agents_available
                    + (CASE WHEN NEW.state = 'AVAILABLE' THEN 1 ELSE 0 END)
                    - (CASE WHEN OLD.state = 'AVAILABLE' THEN 1 ELSE 0 END),
                agents_reserved = agents_reserved
                    + (CASE WHEN NEW.state = 'RESERVED' THEN 1 ELSE 0 END)
                    - (CASE WHEN OLD.state = 'RESERVED' THEN 1 ELSE 0 END),
                agents_dialing = agents_dialing
                    + (CASE WHEN NEW.state = 'DIALING' THEN 1 ELSE 0 END)
                    - (CASE WHEN OLD.state = 'DIALING' THEN 1 ELSE 0 END),
                agents_connected = agents_connected
                    + (CASE WHEN NEW.state = 'CONNECTED' THEN 1 ELSE 0 END)
                    - (CASE WHEN OLD.state = 'CONNECTED' THEN 1 ELSE 0 END),
                agents_wrapup = agents_wrapup
                    + (CASE WHEN NEW.state = 'WRAP_UP' THEN 1 ELSE 0 END)
                    - (CASE WHEN OLD.state = 'WRAP_UP' THEN 1 ELSE 0 END),
                updated_at = now()
            WHERE campaign_id = NEW.campaign_id;
        END IF;
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER agents_counters_trg
AFTER INSERT OR UPDATE OF state ON agents
FOR EACH ROW EXECUTE FUNCTION trg_agents_counters();

CREATE OR REPLACE FUNCTION trg_calls_counters() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO campaign_counters (campaign_id) VALUES (NEW.campaign_id)
            ON CONFLICT (campaign_id) DO NOTHING;
        UPDATE campaign_counters SET
            calls_ringing   = calls_ringing   + (CASE WHEN NEW.state IN ('INITIATED','RINGING') THEN 1 ELSE 0 END),
            calls_connected = calls_connected + (CASE WHEN NEW.state = 'CONNECTED' THEN 1 ELSE 0 END),
            updated_at = now()
        WHERE campaign_id = NEW.campaign_id;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.state IS DISTINCT FROM NEW.state THEN
            UPDATE campaign_counters SET
                calls_ringing = calls_ringing
                    + (CASE WHEN NEW.state IN ('INITIATED','RINGING') THEN 1 ELSE 0 END)
                    - (CASE WHEN OLD.state IN ('INITIATED','RINGING') THEN 1 ELSE 0 END),
                calls_connected = calls_connected
                    + (CASE WHEN NEW.state = 'CONNECTED' THEN 1 ELSE 0 END)
                    - (CASE WHEN OLD.state = 'CONNECTED' THEN 1 ELSE 0 END),
                updated_at = now()
            WHERE campaign_id = NEW.campaign_id;
        END IF;
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER calls_counters_trg
AFTER INSERT OR UPDATE OF state ON calls
FOR EACH ROW EXECUTE FUNCTION trg_calls_counters();
