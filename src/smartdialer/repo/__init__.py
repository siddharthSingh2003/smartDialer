from .agents import AgentRepo
from .borrowers import BorrowerRepo
from .calls import CallRepo
from .decisions import DecisionRepo
from .events import EventRepo
from .metrics import MetricsRepo


class Repos:
    """Bag-of-repos, wired once and passed around. Keeps callers from
    constructing each repo individually."""

    def __init__(self, db):
        self.db = db
        self.agents = AgentRepo()
        self.borrowers = BorrowerRepo()
        self.calls = CallRepo()
        self.events = EventRepo()
        self.decisions = DecisionRepo(db)
        self.metrics = MetricsRepo()
