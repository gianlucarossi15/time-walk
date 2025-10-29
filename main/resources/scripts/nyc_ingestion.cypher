MATCH(n) DETACH DELETE n;

// nycBikes nodes
CALL apoc.periodic.iterate(
  "CALL apoc.load.json('file:///nycBikes/graph_nodes.json') YIELD value RETURN value",
  "
  MERGE (s:Station { stationId: value.station_id })
SET s += {
  nodeId: value.nodeid,
  name: value.name,
  short_name: value.short_name,
  region_id: value.region_id,
  capacity: value.capacity,
  lat: value.lat,
  lon: value.lon,
  start: datetime(value.start),
  end: datetime(value.end)
}
WITH s, value.ts AS ts
SET s.num_bikes_available_values      = [x IN ts.num_bikes_available[..500] | toFloat(x.Value)],
    s.num_bikes_available_timestamps  = [x IN ts.num_bikes_available[..500] | datetime(x.Start)],
    s.num_docks_disabled_values       = [x IN ts.num_docks_disabled[..500] | toFloat(x.Value)],
    s.num_docks_disabled_timestamps   = [x IN ts.num_docks_disabled[..500] | datetime(x.Start)],
    s.num_bikes_disabled_values       = [x IN ts.num_bikes_disabled[..500] | toFloat(x.Value)],
    s.num_bikes_disabled_timestamps   = [x IN ts.num_bikes_disabled[..500] | datetime(x.Start)],
    s.num_ebikes_available_values     = [x IN ts.num_ebikes_available[..500] | toFloat(x.Value)],
    s.num_ebikes_available_timestamps = [x IN ts.num_ebikes_available[..500] | datetime(x.Start)]
  ",
  {batchSize:1000, parallel:true}
);

// nycBikes edges
CALL apoc.periodic.iterate(
  "CALL apoc.load.json('file:///nycBikes/graph_edges.json') YIELD value RETURN value",
  "
    MATCH (source:Station { stationId: value.from }),
          (target:Station { stationId: value.to })
    MERGE (source)-[r:SUPER_EDGE { start: value.start, end: value.end }]->(target)
    WITH r, value.ts AS ts
    SET
      r.num_bikes_values         = [x IN ts.num_bikes[..500]       | toFloat(x.Value)],
      r.num_bikes_timestamps     = [x IN ts.num_bikes[..500]       | datetime(x.Start)],
      r.member_rides_values      = [x IN ts.member_ride[..500]     | toFloat(x.Value)],
      r.member_rides_timestamps  = [x IN ts.member_ride[..500]     | datetime(x.Start)],
      r.casual_rides_values      = [x IN ts.casual_ride[..500]     | toFloat(x.Value)],
      r.casual_rides_timestamps  = [x IN ts.casual_ride[..500]     | datetime(x.Start)],
      r.classic_rides_values     = [x IN ts.classic_ride[..500]    | toFloat(x.Value)],
      r.classic_rides_timestamps = [x IN ts.classic_ride[..500]    | datetime(x.Start)],
      r.electric_rides_values    = [x IN ts.electric_ride[..500]   | toFloat(x.Value)],
      r.electric_rides_timestamps= [x IN ts.electric_ride[..500]   | datetime(x.Start)],
      r.active_trips_values      = [x IN ts.active_trips[..500]    | toFloat(x.Value)],
      r.active_trips_timestamps  = [x IN ts.active_trips[..500]    | datetime(x.Start)]
  ",
  {
    batchSize: 200,
    parallel: false
  }
);

// indexes
CREATE INDEX station_nodeId IF NOT EXISTS
FOR (s:Station) ON (s.nodeId);

CREATE INDEX station_num_bikes_available IF NOT EXISTS
FOR (s:Station) ON (s.num_bikes_available_values, s.num_bikes_available_timestamps);

CREATE INDEX station_num_docks_disabled IF NOT EXISTS
FOR (s:Station) ON (s.num_docks_disabled_values, s.num_docks_disabled_timestamps);

CREATE INDEX station_num_bikes_disabled IF NOT EXISTS
FOR (s:Station) ON (s.num_bikes_disabled_values, s.num_bikes_disabled_timestamps);

CREATE INDEX station_num_ebikes_available IF NOT EXISTS
FOR (s:Station) ON (s.num_ebikes_available_values, s.num_ebikes_available_timestamps);
