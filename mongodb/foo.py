from mongodb.main import Factory

row = [{"response_time": 8.816985964775085, "event_start_ms": 980, "event_start_epoc": 1538708772, "year": 2018, "month": 10, "day": 5},
       {"response_time": 11.618869066238403, "event_start_ms": 964, "event_start_epoc": 1538708773, "year": 2018, "month": 10, "day": 5},
       {'response_time': 18.78785899281502, 'event_start_ms': 15, 'event_start_epoc': 1544956976, 'year': 2018, 'month': 12, 'day': 16},
       {'response_time': 20.88376799225807, 'event_start_ms': 14, 'event_start_epoc': 1544956976, 'year': 2018, 'month': 12, 'day': 16}]

Factory().run(row)