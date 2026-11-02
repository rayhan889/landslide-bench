#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  namenode)
    if [ ! -d /data/hdfs/name/current ]; then
      hdfs namenode -format -nonInteractive -force
    fi
    exec hdfs namenode ;;
  datanode)
    exec hdfs datanode ;;
  *)
    exec "$@" ;;
esac
