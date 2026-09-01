| Scenario | Mean utilization | Calls connected | Abandon % | Dominant reason code |
|---|---|---|---|---|
| A | 98.6% | 97 | 0.00% | NO_AGENTS |
| B | 99.3% | 139 | 0.00% | NO_AGENTS |
| C | 99.6% | 67 | 0.00% | NO_AGENTS |
| D | 99.3% | 106 | 0.00% | NO_AGENTS |
| E_outage | 99.2% | 137 | 0.00% | NO_AGENTS |
| F_agentdrop | 99.3% | 138 | 0.00% | NO_AGENTS |
| G_progressive_baseline | 99.1% | 140 | 0.00% | OK |

### Predictive (B) vs progressive baseline (G), same seed

| | progressive | predictive | delta |
|---|---|---|---|
| utilization | 99.1% | 99.3% | +0.2pp |
| calls connected | 140 | 139 | -1 |
| abandonment | 0.00% | 0.00% | within 0.00% budget (3%) |
| safety interventions | 0 | 923 | of 1199 ticks |
