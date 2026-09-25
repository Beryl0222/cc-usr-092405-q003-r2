"use strict";

const { spawnSync } = require("node:child_process");

const result = spawnSync("python3", ["-m", "unittest", "-v",
  "service_contract",
  "test_engine_timeline",
  "test_mission_sync",
  "test_slice_conflict",
  "test_http_api",
], { stdio: "inherit" });
if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
process.exit(result.status ?? 1);
