rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo1:27017", priority: 2 },
    { _id: 1, host: "mongo2:27017", priority: 1 },
    { _id: 2, host: "mongo3:27017", priority: 1 }
  ]
});

sleep(15000);

// Step 3: Check status
rs.status();

cfg = rs.conf();
cfg.members[0].host = "mongo1:27017";
cfg.members[1].host = "mongo2:27017";
cfg.members[2].host = "mongo3:27017";
rs.reconfig(cfg, {force: true});

sleep(10000);
rs.status();