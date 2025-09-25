
from datetime import timedelta
from opencep.base.Pattern import Pattern
from opencep.base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from opencep.condition.CompositeCondition import AndCondition
from opencep.condition.BaseRelationCondition import EqCondition
from opencep.condition.Condition import Variable, SimpleCondition
from opencep.condition.KCCondition import KCIndexCondition
from src.executor.runner import Runner

# PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
# WHERE a[i+1].bike = a[i].bike AND b.end in {7,8,9} AND a[last].bike = b.bike AND a[i+1].start = a[i].end
# WITHIN 1h
# RETURN (a[1].start, a[i].end, b.end)
structure = SeqOperator(
    KleeneClosureOperator(PrimitiveEventStructure(event_type="BikeTrip", name="a")),
    PrimitiveEventStructure(event_type="BikeTrip", name="b")
)

same_bike_chain = KCIndexCondition(
    names={"a"},
    getattr_func=lambda ev: ev["bike_id"],
    relation_op=lambda curr, prev: curr == prev,
    offset=-1
)

chain_contiguity = KCIndexCondition(
    names={"a"},
    getattr_func=lambda ev: (ev["start_station_id"], ev["end_station_id"]),
    relation_op=lambda curr, prev: curr[0] == prev[1],
    offset=-1
)
last_a_bike_vs_b_bike = EqCondition(
    Variable("b", lambda ev: ev["bike_id"]),
    Variable("a", lambda ev: ev["bike_id"]),  # engine binds this to the last 'a' in the KC by default
)


b_in_targets = SimpleCondition(
    Variable("b", lambda ev: ev["end_station_id"]),
    relation_op=lambda s: s in {7, 8, 9}
)


hot_paths_pattern = Pattern(
    structure,
    AndCondition(same_bike_chain, chain_contiguity, last_a_bike_vs_b_bike, b_in_targets),
    timedelta(hours=1)
)

def execute(pattern=hot_paths_pattern, bursty=False, limit=100):
    try:
        runner = Runner(pattern=pattern, bursty=bursty, limit=100)
        runner.run()
    except:
        pass

