def reserve_until(clock, cfg) -> float:
    return clock.now() + cfg.agent_reserve_lease_s


def dial_until(clock, cfg) -> float:
    return clock.now() + cfg.call_setup_lease_s
