
from datetime import timedelta
from opencep.base.Pattern import Pattern
from opencep.base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from opencep.condition.CompositeCondition import AndCondition
from opencep.condition.BaseRelationCondition import EqCondition
from opencep.condition.Condition import Variable, SimpleCondition
from opencep.condition.KCCondition import KCIndexCondition
from opencep.misc.ConsumptionPolicy import *
from src.executor.runner import Runner

# PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
# WHERE a[i+1].bike = a[i].bike AND b.end in {7,8,9} AND a[last].bike = b.bike AND a[i+1].start = a[i].end
# WITHIN 1h
# RETURN (a[1].start, a[i].end, b.end)
structure = SeqOperator(
    KleeneClosureOperator(
        PrimitiveEventStructure(event_type="BikeTrip", name="a"),
        min_size=1,
        max_size=5,          # <<< important
    ),
    PrimitiveEventStructure(event_type="BikeTrip", name="b"),
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

# Fix: KC 'a' is a list -> fetch last element's bike_id
last_a_bike_vs_b_bike = EqCondition(
    Variable("b", lambda ev: ev["bike_id"]),
    Variable("a", lambda lst: lst[-1]["bike_id"]),
)

# b_in_targets = SimpleCondition(
#     Variable("b", lambda ev: ev["end_station_id"]),
#     relation_op=lambda s: s in {7, 8, 9}
# )
b_last_digit_in_7_8_9 = SimpleCondition(
    Variable("b", lambda ev: (ev["end_station_id"] % 10) if ev.get("end_station_id") is not None else None),
    relation_op=lambda d: d in {7, 8, 9}
)

consumption = ConsumptionPolicy(
    primary_selection_strategy=SelectionStrategies.MATCH_ANY,
    secondary_selection_strategy=SelectionStrategies.MATCH_NEXT,
    single=["BikeTrip"]   # apply to all BikeTrip events (your whole pattern)
)

hot_paths_pattern = Pattern(
    structure,
    AndCondition(same_bike_chain, chain_contiguity, last_a_bike_vs_b_bike, b_last_digit_in_7_8_9),
    timedelta(hours=1),
    consumption_policy=consumption
)



# hot_paths_pattern = Pattern(
#     structure,
#     AndCondition(same_bike_chain, chain_contiguity, last_a_bike_vs_b_bike, b_in_targets),
#     timedelta(hours=1)
# )


def execute(pattern=hot_paths_pattern, bursty=False, limit=None):
    try:
        runner = Runner(pattern=pattern, bursty=bursty, limit=limit)
        runner.run()
    except:
        pass

